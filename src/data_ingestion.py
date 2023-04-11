import requests
import json

'''
to-do:

- function to get date between specified ranges
- schedule multiple jobs and make sure concurrency limit is not exceeded
- function to automatically ingest data in a certain fixed interval
'''


# data_ingest

class DataIngest:
    def __init__(self, scope: str, key: str):
        self.scope = scope
        self.key = key
        self.api_key = dbutils.secrets.get(scope=self.scope, key=self.key)

    def get_data_by_range(self, start_date, end_date):
        


params_dict = {
    "start_date": "01-01-2023",
    "end_date": "04-04-2023",
    "api_key": "<api_key>",
    "destination_s3_bucket": "allstar-training-mootech",
    "destination_s3_directory": "raw_data/clickstream/2020",
    "table": "clickstream"
}
payload = json.dumps(params_dict)
response = requests.post('https://en44bq5e33.execute-api.us-east-1.amazonaws.com/dev/fetch_data', data=payload)
print(response.text)

# check status

job_status = requests.get('https://en44bq5e33.execute-api.us-east-1.amazonaws.com/dev/job_status',
                          data=json.dumps({'job_id': 147841}))
print(job_status.json())
