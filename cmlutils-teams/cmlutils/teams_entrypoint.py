import logging
import os
import sys
import time
from configparser import ConfigParser, NoOptionError
from json import dump
import json
from logging.handlers import RotatingFileHandler

import click

from cmlutils import constants
from cmlutils.constants import (
    API_V1_KEY,
    CA_PATH_KEY,
    OUTPUT_DIR_KEY,
    URL_KEY,
    USERNAME_KEY,
)
from cmlutils.directory_utils import get_project_metadata_file_path, get_teams_metadata_file_path
from cmlutils.project_entrypoint import _read_config_file as _project_read_config_file, _configure_project_command_logging
from cmlutils.projects import ProjectExporter, ProjectImporter
from cmlutils.script_models import ValidationResponseStatus
from cmlutils.teams import TeamExporter, TeamImporter, _check_teams_users_integration
from cmlutils.utils import (
    compare_metadata,
    get_absolute_path,
    parse_runtimes_v2,
    read_json_file,
    update_verification_status,
    write_json_file,
    fetch_project_names_from_csv
)


def _read_config_file(configfile:str):
    return _project_read_config_file(configfile, "DEFAULT")

def  _configure_command_logging(log_filedir):
    _configure_project_command_logging(log_filedir, "")

@click.group(name="team")
def team_cmd():
    """
    Sub-entrypoint for team command
    """

@team_cmd.command(name="export")
@click.option(
    "--verify",
    "-v",
    is_flag=True,
    help="Flag to automatically trigger migration validation after import.",
)
def team_export_cmd(verify:bool):
    # get export configuration
    config = _read_config_file(
        os.path.expanduser("~") + "/.cmlutils/export-config.ini"
    )
    username = config[USERNAME_KEY]
    url = config[URL_KEY]
    apiv1_key = config[API_V1_KEY]
    output_dir = config[OUTPUT_DIR_KEY]
    ca_path = config[CA_PATH_KEY]

    output_dir = get_absolute_path(output_dir)
    ca_path = get_absolute_path(ca_path)

    log_filedir = os.path.join(output_dir, "logs")
    if (not os.path.exists(log_filedir)):
        os.mkdir(log_filedir)

    _configure_command_logging(output_dir)
    logging.info("Started exporting team")
    try:
        pexport = TeamExporter(
            host=url,
            username=username,
            api_key=apiv1_key,
            top_level_dir=output_dir,
            ca_path=ca_path,
        )
        start_time = time.time()
        exported_data =  pexport._export_team_metadata()

        if (verify):
            user_list = exported_data['user_list']
            team_list = exported_data['team_list']
            result = _check_teams_users_integration(user_list, team_list)
            update_verification_status(not result, "Team Verification")

        print("\033[32m✔ Export of Teams Successful \033[0m".format())
        print(
            "\033[34m\tExported {} Users {}\033[0m".format(
                exported_data.get("total_users"), exported_data.get("user_name_list")
            )
        )
        print(
            "\033[34m\tExported {} Teams {}\033[0m".format(
                exported_data.get("total_teams"), exported_data.get("team_name_list")
            )
        )
        end_time = time.time()
        export_file = log_filedir + constants.EXPORT_METRIC_FILE
        write_json_file(file_path=export_file, json_data=exported_data)
        print(
            "Team Export took {:.2f} seconds".format(
                (end_time - start_time)
            )
        )
    except:
        logging.error("Exception:", exc_info=1)
        exit()


@team_cmd.command(name="import")
@click.option(
    "--verify",
    "-v",
    is_flag=True,
    help="Flag to automatically trigger migration validation after import.",
)
def team_import_cmd(verify):
    config = _read_config_file(
        os.path.expanduser("~") + "/.cmlutils/import-config.ini"
    )

    username = config[USERNAME_KEY]
    url = config[URL_KEY]
    apiv1_key = config[API_V1_KEY]
    local_directory = config[OUTPUT_DIR_KEY]
    ca_path = config[CA_PATH_KEY]
    local_directory = get_absolute_path(local_directory)
    ca_path = get_absolute_path(ca_path)
    log_filedir = os.path.join(local_directory, "", "logs")
    if (not os.path.exists(log_filedir)):
        os.mkdir(log_filedir)

    _configure_command_logging(log_filedir)

    pimport = TeamImporter(
            host=url,
            username=username,
            api_key=apiv1_key,
            top_level_dir=local_directory,
            ca_path=ca_path,
        )

    logging.info("Begin pre-verification of team")
    result = pimport._pre_import_verify()
    update_verification_status(not result, "Team Import Pre-Verification")
    if (not result):
        raise RuntimeError("Team Import Pre-Verification error")

    start_time = time.time()
    import_data = pimport._import_team_metadata()

    print("\033[32m✔ Import of Teams Successful \033[0m")
    print(
            "\033[34m\tImported {} Jobs {}\033[0m".format(
                import_data.get("total_teams"), import_data.get("team_name_list")
            )
    )
    end_time = time.time()
    import_file = log_filedir + constants.IMPORT_METRIC_FILE
    write_json_file(file_path=import_file, json_data=import_data)
    print(
            "Team Import took {:.2f} seconds".format(
                (end_time - start_time)
            )
    )

    if (verify):
        _team_verify()

@team_cmd.command(name="verify")
def team_verify_cmd():
    _team_verify()

def _team_verify():
    config = _read_config_file(
        os.path.expanduser("~") + "/.cmlutils/import-config.ini"
    )

    username = config[USERNAME_KEY]
    url = config[URL_KEY]
    apiv1_key = config[API_V1_KEY]
    local_directory = config[OUTPUT_DIR_KEY]
    ca_path = config[CA_PATH_KEY]
    local_directory = get_absolute_path(local_directory)
    ca_path = get_absolute_path(ca_path)
    log_filedir = os.path.join(local_directory, "", "logs")
    if (not os.path.exists(log_filedir)):
        os.mkdir(log_filedir)

    _configure_command_logging(log_filedir)
    logging.info("Begin Team Verification")

    pimport = TeamImporter(
        host=url,
        username=username,
        api_key=apiv1_key,
        top_level_dir=local_directory,
        ca_path=ca_path,
    )

    imported_team_data = pimport.teams
    imported_team_name_list = sorted(pimport._namelist(imported_team_data))

    config = _read_config_file(
        os.path.expanduser("~") + "/.cmlutils/export-config.ini"
    )

    export_username = config[USERNAME_KEY]
    export_url = config[URL_KEY]
    export_apiv1_key = config[API_V1_KEY]
    output_dir = config[OUTPUT_DIR_KEY]
    ca_path = config[CA_PATH_KEY]
    export_output_dir = get_absolute_path(output_dir)
    export_ca_path = get_absolute_path(ca_path)

    pexport = TeamExporter(
        host=export_url,
        username=export_username,
        api_key=export_apiv1_key,
        top_level_dir=export_output_dir,
        ca_path=export_ca_path,
    )
    # set to True if you want to use local cache to check
    if False:
        filepath = get_teams_metadata_file_path(
            top_level_dir=local_directory
        )
        exported_team_data = read_json_file(filepath)
    else:
        exported_team_data = pexport.get_teamuser_listv1(True)
    exported_team_name_list =sorted(pexport._namelist(exported_team_data))
    team_diff, team_config_diff = compare_metadata(
        imported_team_data,
        exported_team_data,
        imported_team_name_list,
        exported_team_name_list,
        skip_field=["id", "name", "username_hash", "joined_on", "password_updated_at", "followers", "public_projects",
                    "organization_projects", "private_projects", "private_projects", "running_dashboards", "members",
                    "api_keys", "banned", "deactivated", "namespace", "memory_hours", "cpu_hours", "gpu_hours",
                    "avg_session_duration",
                    "jobs_run", "sessions_run", "html_url", "ldapSynced", "lastSyncedAt", "teamMembers", "url"],
        user_id_key='username',
    )
    logging.info("Source      Team list {}".format(exported_team_name_list))
    logging.info("Destination Team list {}".format(imported_team_name_list))
    logging.info(
        "All Teams in source project is present at destination ".format(
            team_diff
        )
        if not team_diff
        else "Team {} Not Found in source and destination".format(team_diff)
    )
    logging.info(
        "No Team Config Difference Found"
        if not team_config_diff
        else "Difference in Team Config {}".format(team_config_diff)
    )

    members_diff = []
    members_config_diff = []

    for exported_team in exported_team_data:
        team_name = exported_team['username']
        imported_team = None
        try:
            imported_team = pimport.teamdict[team_name]
        except KeyError as e:
            continue

        exported_member_data  = exported_team['teamMembers']
        exported_member_name_list = sorted(pexport._namelist(exported_member_data))
        imported_member_data = imported_team['teamMembers']
        imported_member_name_list = sorted(pexport._namelist(imported_member_data))
        member_diff, member_config_diff = compare_metadata(
            imported_member_data,
            exported_member_data,
            imported_member_name_list,
            exported_member_name_list,
            skip_field=['id','html_url','url',"permission"]
        )
        logging.info("Source      Member list {} for team {}".format(exported_member_name_list, team_name))
        logging.info("Destination Member list {} for team {}".format(imported_member_name_list, team_name))
        logging.info(
            "All Members in source is present at destination {} ".format(
                team_name
            )
            if not member_diff
            else "Members {} Not Found in source and destination {}".format(member_diff, team_name)
        )
        logging.info(
            "No Members Config Difference Found"
            if not member_config_diff
            else "Difference in Members Config {} for team {}".format(member_config_diff, team_name)
        )
        update_verification_status(
            True if (member_diff or member_config_diff) else False,
            message="Members Verification for team {}".format(team_name),
        )
        members_diff.append(member_diff)
        members_config_diff.append(member_config_diff)

    result = [team_diff, team_config_diff]
    result.extend(members_diff)
    result.extend(members_config_diff)
    print(result)
    migration_status = all(not sublist for sublist in result)
    update_verification_status(
        not migration_status,
        message="Team Vefification status is".format(),
    )

@team_cmd.command(name="purge")
def team_verify_cmd():
    config = _read_config_file(
        os.path.expanduser("~") + "/.cmlutils/import-config.ini"
    )

    username = config[USERNAME_KEY]
    url = config[URL_KEY]
    apiv1_key = config[API_V1_KEY]
    local_directory = config[OUTPUT_DIR_KEY]
    ca_path = config[CA_PATH_KEY]
    local_directory = get_absolute_path(local_directory)
    ca_path = get_absolute_path(ca_path)
    log_filedir = os.path.join(local_directory, "", "logs")
    if (not os.path.exists(log_filedir)):
        os.mkdir(log_filedir)

    _configure_command_logging(log_filedir)

    pimport = TeamImporter(
            host=url,
            username=username,
            api_key=apiv1_key,
            top_level_dir=local_directory,
            ca_path=ca_path,
        )
    logging.info("Begin Purge teams")
    pimport._drop_teams()
    logging.info("End Purge teams")

