import json
import logging
import os
import signal
import subprocess
import urllib.parse
from datetime import datetime, timedelta
from encodings import utf_8
from string import Template
from sys import stdout
from typing import Any

import requests
from requests import HTTPError

from cmlutils import constants, legacy_engine_runtime_constants
from cmlutils.base import BaseWorkspaceInteractor
from cmlutils.cdswctl import cdswctl_login, obtain_cdswctl
from cmlutils.constants import ApiV1Endpoints, ApiV2Endpoints, MEMBER_MAPV1
from cmlutils.directory_utils import (
    ensure_project_data_and_metadata_directory_exists,
    get_applications_metadata_file_path,
    get_jobs_metadata_file_path,
    get_models_metadata_file_path,
    get_project_collaborators_file_path,
    get_project_data_dir_path,
    get_project_metadata_file_path, get_users_metadata_file_path, get_teams_metadata_file_path,
)
from cmlutils.ssh import open_ssh_endpoint
from cmlutils.utils import (
    call_api_v1,
    call_api_v2,
    extract_fields,
    find_runtime,
    flatten_json_data,
    get_best_runtime,
    read_json_file,
    write_json_file,
)


# check whether all members in teams are present in user
def _check_teams_users_integration(user_list:list, team_list:list):
    result = True
    user_dict = {}
    for user in user_list:
        username = user['username']
        user_dict[username] = user
        
    for team in team_list:
        member_list = team['teamMembers']
        team_name = team['username']
        team_owner_list = list(filter(lambda m: m['permission'] == 'owner', member_list))
        if len(team_owner_list) <= 0 :
            logging.error("team {} should have at least one team owner".format(team_name))
            result = False
        if len(team_owner_list) > 1:
           team_owner_name_list = list(map(lambda t: t['username'], team_owner_list))
           logging.warn("team {} have more than one owners {},it will cause conflicts of owner in target cml workspace".format(team_name, team_owner_name_list))
        team_owner_name = team_owner_list[0]['username']
        if team_owner_name in user_dict:
          team_owner = user_dict[team_owner_name]
          if not team_owner['admin']:
            logging.warning("team owner {} of team {} is not ML Admin, doesn't have permission to add team".format(team_owner_name,team_name))
            # result = False
        else:
            logging.error("user {} in team {} is not present in user list".format(team_owner_name,team_name))
            result = False
        
        for member in member_list:
            username = member['username']
            if not username in user_dict:
                logging.error("user {} in team {} is not present in user list".format(username, team_name))
                result = False
    return result

class TeamBase(BaseWorkspaceInteractor):
    def __init__(
        self,
        host: str,
        username: str,
        api_key: str,
        top_level_dir: str,
        ca_path: str,
    ) -> None:
        self.top_level_dir = top_level_dir
        self.project_name='DEFAULT'
        self.key_cache = {}
        super().__init__(host, username, self.project_name, api_key, ca_path, "")
        self.metrics_data = dict()
    
    def _new_apiv2_key(self, username: str) -> str:
        endpoint = Template(ApiV1Endpoints.API_KEY.value).substitute(
            username=username
        )
        json_data = {
            "expiryDate": (datetime.now() + timedelta(weeks=1)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        }
        response = call_api_v1(
            host=self.host,
            endpoint=endpoint,
            method="POST",
            api_key=self.api_key,
            json_data=json_data,
            ca_path=self.ca_path,
        )
        response_dict = response.json()
        _apiv2_key = response_dict["apiKey"]
        return _apiv2_key

    def _get_apiv2_key(self, username: str) -> str:
        endpoint = Template(ApiV1Endpoints.API_KEY.value).substitute(
            username=username
        )
        json_data = {
            "expiryDate": (datetime.now() + timedelta(weeks=1)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        }
        response = call_api_v1(
            host=self.host,
            endpoint=endpoint,
            method="GET",
            api_key=self.api_key,
            json_data=json_data,
            ca_path=self.ca_path,
        )
        response_dict = response.json()
        _apiv2_key = response_dict["apiKey"]
        return _apiv2_key

    def _retrive_or_create_api_key_for_user(self, username: str):
        if username in self.key_cache:
            return self.key_cache[username]
        else:
            #try to get key first
            api_key = None
            try:
                api_key = self._get_apiv2_key(username)
            except:
                try:
                    api_key = self._new_apiv2_key(username)
                except:
                    api_key = None
            if not api_key is not None:
                self.key_cache['username'] = api_key
        return api_key
      
    def call_api_v1(
            self,
            endpoint: str,
            method: str,
            json_data: dict = None,
            username = None
    ) -> requests.Response:
        if  (not username is None) and (username != self.username):
            api_key = self._retrive_or_create_api_key_for_user(username)
            return call_api_v1(self.host, endpoint, method,      api_key, json_data, self.ca_path)
        else:
            return call_api_v1(self.host, endpoint, method, self.api_key, json_data, self.ca_path)

    def get_team_or_user_listv1_paged(self, offset:int, limit:int, is_team: bool):
        endpoint = Template(ApiV1Endpoints.USER_TEAM_LIST.value).substitute(
            offset=offset, limit=limit
        )
        if is_team:
            endpoint = endpoint + "&type=organization"
        else:
            endpoint = endpoint + "&type=user"

        response = call_api_v1(
            host=self.host,
            endpoint=endpoint,
            method="GET",
            api_key=self.api_key,
            ca_path=self.ca_path,
        )
        return response.json()

    def get_teamuser_listv1(self, is_team: bool):
        limit = 10000
        page = 0
        user_list = []
        user_list_page = self.get_team_or_user_listv1_paged(page * limit, limit, is_team)
        while(len(user_list_page) > 0):
            page = page + 1
            user_list.extend(user_list_page)
            user_list_page = self.get_team_or_user_listv1_paged(page * limit, limit, is_team)
        return user_list

    def _add_teamv1(self, team: dict, username = None):
        endpoint = Template(ApiV1Endpoints.ADD_TEAM.value).substitute(
        )
        logging.debug("user:{} data{}".format(username,team))
        response = self.call_api_v1(
            endpoint=endpoint,
            method="POST",
            json_data=team,
            username = username
        )
        return response.json()

    def _del_teamv1(self, team: dict, username = None):
        team_name = team["username"]
        endpoint = Template(ApiV1Endpoints.DEL_TEAM.value).substitute(
            team = team_name
        )

        response = self.call_api_v1(
            endpoint=endpoint,
            method="DELETE",
            username = username
        )
        return response.status_code

    def _add_team(self, team: dict, username = None):
        team_name = team['username']
        logging.info("Begin add team {} as user {}".format(team_name, username))
        self._add_teamv1(team, username)

    @staticmethod
    def _namelist(l: list):
        return list(map(lambda d:d['username'], l))

    def _del_team(self, team: dict):
        try:
            self._del_teamv1(team)
        except:
            pass

    def _add_team_memberv1(self, team_name: str, member: dict, username = None):
        endpoint = Template(ApiV1Endpoints.TEAM_MEMBER_ADD.value).substitute(
            team = team_name
        )

        response = self.call_api_v1(
            endpoint=endpoint,
            method="POST",
            json_data=member,
            username = username
        )
        return response.json()

    def add_team_member(self, team_name: str, member: dict, username = None):
        member_name = member['username']
        logging.info("Begin add member {} to team {}".format(member_name, team_name))
        try:
            self._add_team_memberv1(team_name, member, username)
        except HTTPError as e:
            resp = e.response
            cause = str(resp.json()['message'])
            if (cause.find("is already a member of this team") < 0):
                raise
            else:
                logging.warning("ignore error since member {} is already added to team {}".format(member_name, team_name))

class TeamExporter(TeamBase):
    def __init__(
        self,
        host: str,
        username: str,
        api_key: str,
        top_level_dir: str,
        ca_path: str,
    ) -> None:
        super().__init__(host, username, api_key, top_level_dir, ca_path)

    def _export_users(self):
        filepath = get_users_metadata_file_path(
            top_level_dir=self.top_level_dir
        )
        logging.info("Exporting user metadata to path %s", filepath)
        user_list = self.get_teamuser_listv1(False)
        user_name_list = list(map(lambda c:c['username'], user_list))
        write_json_file(file_path=filepath, json_data=user_list)
        self.metrics_data["total_users"] = len(user_name_list)
        self.metrics_data["user_name_list"] = sorted(user_name_list)
        self.metrics_data["user_list"] = user_list

    def _export_teams(self):
        filepath = get_teams_metadata_file_path(
            top_level_dir=self.top_level_dir
        )
        logging.info("Exporting team metadata to path %s", filepath)
        team_list = self.get_teamuser_listv1(True)
        team_name_list = list(map(lambda c: c['username'], team_list))
        write_json_file(file_path=filepath, json_data=team_list)
        self.metrics_data["total_teams"] = len(team_name_list)
        self.metrics_data["team_name_list"] = sorted(team_name_list)
        self.metrics_data["team_list"] = team_list

    def _export_team_metadata(self):
        self._export_users()
        self._export_teams()
        return self.metrics_data

class TeamImporter(TeamBase):
    def __init__(
        self,
        host: str,
        username: str,
        api_key: str,
        top_level_dir: str,
        ca_path: str,
    ) -> None:
        super().__init__(host, username, api_key, top_level_dir, ca_path)
        self.ENABLE_TEAM_FAKE_TEST = False
        self.teams = self.get_teamuser_listv1(True)
        self.team_name_list = sorted(self._namelist(self.teams))
        self.teamdict = {}
        for team in self.teams:
            team_name = team['username']
            self.teamdict[team_name] = team


    def _translate_team(self, team:dict):
        team_obj = {"type":"organization"}
        # team_obj['bio'] = team['bio']
        # team_obj['cn'] = ""
        team_obj['username'] = team['username']
        return team_obj

    def _translate_member(self, m: dict):
        member = extract_fields(m, MEMBER_MAPV1)
        return member

    def _empty_team_entry(self, name):
        team = {}
        team['username'] = name
        team['teamMembers'] = []
        return team

    def _create_or_update_team(self, team:dict):
        team_name = team['username']
        member_list = team['teamMembers']
        team_owner_list = list(filter(lambda m: m['permission'] == 'owner', member_list))
        team_owner = team_owner_list[0]
        owner_name = team_owner['username']

        # if not team_name in self.teamdict:
        #     self._add_team(self._translate_team(team), username = owner_name)
        #     self.teamdict[team_name] = self._empty_team_entry(team_name)
        self._del_team(team)
        self._add_team(self._translate_team(team), username = owner_name)
        if not self.ENABLE_TEAM_FAKE_TEST:
            filtered_member_list = list(filter(lambda m: m['permission'] != 'owner', member_list))
            for m in filtered_member_list:
                member = self._translate_member(m)
                self.add_team_member(team_name, member, owner_name)
            for i in range(len(team_owner_list)):
                if i == 0:
                    continue
                else:
                    m = team_owner_list[i]
                    owner_name = m['username']
                    logging.warning("more than one owners for the team {} are found, the owner {} will be converted "
                                    "to admin after migration.".format(team_name, owner_name))
                    m['permission'] = 'admin'
                    member = self._translate_member(m)
                    self.add_team_member(team_name, member, owner_name)

        else:
            filtered_member_list = list(filter(lambda m: m['permission'] != 'owner', member_list))
            if len(filtered_member_list) > 0:
                m = filtered_member_list[0]
                member = self._translate_member(m)
                member['username'] = 'oleg'
                self.add_team_member(team_name, member, owner_name)


    def _pre_import_verify(self):
        filepath = get_teams_metadata_file_path(
            top_level_dir=self.top_level_dir
        )
        team_list = read_json_file(filepath)
        user_list = self.get_teamuser_listv1(False)
        result = _check_teams_users_integration(user_list, team_list)
        return result

    def _import_teams(self):
        filepath = get_teams_metadata_file_path(
            top_level_dir=self.top_level_dir
        )
        team_metadata_list = read_json_file(filepath)
        logging.info("Importing team metadata from path %s", filepath)
        for team in team_metadata_list:
            team_name = team['username']
            self._create_or_update_team(team)

        team_list = self.get_teamuser_listv1(True)
        team_name_list = list(map(lambda c: c['username'], team_list))
        self.metrics_data["total_teams"] = len(team_name_list)
        self.metrics_data["team_name_list"] = sorted(team_name_list)


    def _import_team_metadata(self):
        self._import_teams()
        return self.metrics_data

    def _drop_teams(self):
        filepath = get_teams_metadata_file_path(
            top_level_dir=self.top_level_dir
        )
        team_metadata_list = read_json_file(filepath)
        logging.info("Importing team metadata from path %s", filepath)
        for team in team_metadata_list:
            team_name = team['username']
            logging.info("delete team {}".format(team_name))
            self._del_team(team)