# Copyright (c) 2023 Cloudera, Inc. All rights reserved.
# Author: Cloudera
# Description: An example script to change ownership of the project .


"""
    To Install cmlapi
    pip install <Target-CML-DOMAIN>/api/v2/python.tar.gz
    Please refer <Target-CML-DOMAIN>/api/v2/python
"""
import cmlapi

api_url = "https://ml-a0804dc1-f26.xzhong-c.a465-9q4k.cloudera.site"
api_key = "7b860f402c1f2b0464af438be34803ec0e3415e9574adaf991fa10f1f040909b.f75a3f4821d5012705e2aa42ff7c9688462000ce9a8f56e9a90868cb02f92da9"
projectId = "l0i9-9wnn-63zv-7jfs"

# client setup
config = cmlapi.Configuration()
config.host = api_url
config.verify_ssl = False
client = cmlapi.ApiClient(config)
client.set_default_header("authorization", "Bearer " + api_key)
api = cmlapi.CMLServiceApi(client)

# get details of the project you want to update and print current owner

proj = api.get_project(projectId)
print(
    "================================================BEFORE==========================================="
)
print(proj.owner.username)  # OWNER1 owns the project
print(
    "================================================================================================"
)

# Update the project's ownership
proj.owner.username = "admin"
api.update_project(proj, projectId)

# get details of the updated project
projUpdated = api.get_project(projectId)
print(
    "================================================AFTER==========================================="
)
print(projUpdated.owner.username)  # onwer2 owns the project now
print(
    "================================================================================================"
)
