"""
Before using this script, the following secrets need to be configured within your repo:
- DBT_CLOUD_SERVICE_TOKEN
- DBT_CLOUD_API_KEY
- DBT_CLOUD_ACCOUNT_ID
- DD_API_KEY --> API key from datadog, used indirectly as an env variable in datadog_api_client.v2.Configuration

The following are configured within the action itself but would need to be added as an environment variable if running as a one-off.
- DBT_CLOUD_JOB_ID
- DD_SITE --> most likely will be datadoghq.com, used indirectly as an env variable in datadog_api_client.v2.Configuration
"""

# stdlib
import json
import os
from typing import List

# third party
from datadog_api_client.v2 import ApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.http_log import HTTPLog
from datadog_api_client.v2.model.http_log_item import HTTPLogItem
from dbtc import dbtCloudClient as dbtc


# Maximum array size from datadog docs
MAX_LIST_SIZE = 1000

# List of resources to pull metadata for
# Exhaustive list is models, tests, sources, snapshots, macros, exposures, metrics, seeds
RESOURCES = ['models', 'tests', 'sources', 'snapshots']


def chunker(seq):
    """Ensure that the log array is <= to the MAX_LIST_SIZE)"""
    size = MAX_LIST_SIZE
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def send_logs(body: List[HTTPLogItem]):
    body = HTTPLog(body)
    configuration = Configuration()
    with ApiClient(configuration) as api_client:
        api_instance = LogsApi(api_client)
        response = api_instance.submit_log(body=body, content_encoding='gzip')
        return response


if __name__ == '__main__':
    
    logs = []
    account_id = os.getenv('DBT_CLOUD_ACCOUNT_ID')
    job_id = os.getenv('DBT_CLOUD_JOB_ID')
    
    # Initialize client with an API key and service token
    client = dbtc(
        service_token=os.getenv('DBT_CLOUD_SERVICE_TOKEN'),
    )
    
    # Trigger Job and Poll until successful
    run = client.cloud.trigger_job(
        account_id, job_id, {'cause': 'Triggered via GH actions'}
    )
    
    run_id = run['data']['id']
    
    # Retrieve all resources defined above via metadata API
    for resource in RESOURCES:
        method = f'get_{resource}'
        data = getattr(client.metadata, method)(
            job_id=job_id, run_id=run_id
        )['data'][resource]
        for datum in data:
            logs.append(HTTPLogItem(
                ddsource='python',
                ddtags=f'job:daily_job,resource:{resource}',
                hostname='cloud.getdbt.com',
                message=json.dumps(datum),
                service='gh_actions'
            ))
    
    for log_items in chunker(logs):
        send_logs(log_items)
