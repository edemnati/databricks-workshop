# Databricks notebook source
#import os
#os.environ['DATABRICKS_HOST'] = 'https://eastus2.azuredatabricks.net/'
#os.environ['DATABRICKS_TOKEN'] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# COMMAND ----------

new_cluster_config = {
  "spark_version": "7.3.x-scala2.12",
  "node_type_id": "Standard_D3_v2",
  "num_workers": 2
}

# Existing cluster ID where integration test will be executed
existing_cluster_id = '0125-194308-oyr46uje'
# Path to the notebook with the integration test
notebook_path = '/notebooks/day1_notebook'
repo_path = '/Repos/ezzatdemnati@microsoft.com/ip_project'


repos_path_prefix='/Repos/ezzatdemnati@microsoft.com/ip_project'
git_url = 'https://dev.azure.com/ezzatdemnati-ip/ip_project/_git/ip_project'
provider = 'gitHub'
branch = 'master'

# COMMAND ----------

from argparse import ArgumentParser
import sys
p = ArgumentParser()

p.add_argument("--branch_name", required=False, type=str)
#p.add_argument("--pr_branch", required=False, type=str)

namespace = p.parse_known_args(sys.argv + [ '', ''])[0]
branch_name = namespace.branch_name
print('Branch Name: ', branch_name)
#pr_branch = namespace.pr_branch
#print('PR Branch: ', pr_branch)

# COMMAND ----------

import os
import json
import time
from datetime import datetime

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.repos.api import ReposApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi

# Let's create Databricks CLI API client to be able to interact with Databricks REST API
api_client = ApiClient(
  host  = os.getenv('DATABRICKS_HOST'),
  token = os.getenv('DATABRICKS_TOKEN')
)
#Let's checkout the needed branch
#if branch_name == 'merge':
#  branch = pr_branch
#else:
#  branch = branch_name
branch = branch_name
print('Using branch: ', branch)
  
#Let's create Repos Service
repos_api = ReposApi(api_client)

# Let's store the path for our new Repo
_b = branch.replace('/','_')
#repo_path = f'{repos_path_prefix}_{_b}_{str(datetime.now().microsecond)}'
print('Checking out the following repo: ', repo_path)

# Let's clone our GitHub Repo in Databricks using Repos API
#repo = repos_api.create(url=git_url, provider=provider, path=repo_path)

#Get 
repo_id = repo_id = repos_api.get_repo_id(repos_path_prefix)
print(f"rRepo path:{repos_path_prefix}, Repo ID:{repo_id}")

try:
    # Checks out the repo to the given branch or tag.
    repos_api.update(repo_id=repo_id, branch=branch,tag=None)

    #Let's create a jobs service to be able to start/stop Databricks jobs
    jobs_api = JobsApi(api_client)
    runs_api = RunsApi(api_client)

    notebook_task = {'notebook_path': repo_path + notebook_path}
    #new_cluster = json.loads(new_cluster_config)

    # Submit integration test job to Databricks REST API
    run_data = {}
    run_data["run_name"]="Test run Job"
    #run_data['existing_cluster_id'] = existing_cluster_id
    run_data['new_cluster'] = new_cluster_config
    #run_data['libraries'] = libraries
    run_data['notebook_task'] = notebook_task
    
    res = runs_api.submit_run(json=run_data )
    run_id = res['run_id']
    print(run_id)

    #Wait for the job to complete
    while True:
        run_status = runs_api.get_run(run_id)
        run_output = runs_api.get_run_output(run_id)
        print(F"run_status:{run_status}")
        result_state = run_status["state"].get("result_state", None)
        if result_state:
            print(result_state)
            print(F"run_output:{run_output}")
            assert result_state == "SUCCESS"
            break
        else:
            time.sleep(5)
finally:
    #repos_api.delete()(id=repo['id'])
    print("OK")
