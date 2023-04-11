import requests
import json
import datetime
from datetime import timedelta

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

    def get_data_by_range(self, start_date: datetime.datetime,
                          end_date: datetime.datetime = datetime.datetime.today() - timedelta(days=1),
                          table: str = None):
        """
        Retrieves data from the given start_date to end_date (by default end_date is yesterday)
        """
        start_date = start_date  # to be edited
        for year in range(start_date.year, end_date.year + 1):
            if year > start_date.year:
                start_month, start_day = 1, 1
            else:
                start_month, start_day = start_date.month, start_date.day

            if year < end_date.year:
                end_month, end_day = 12, 31
            else:
                end_month, end_day = end_date.month, end_date.day

            first_day = datetime.date(year, start_month, start_day)
            last_day = datetime.date(year, end_month, end_day)
            self.make_request(first_day, last_day, table)

    def make_request(self, start_date, end_date, table):
        params_dict = {
            "start_date": start_date,
            "end_date": end_date,
            "api_key": self.api_key,
            "destination_s3_bucket": "allstar-training-mootech",
            "destination_s3_directory": f"raw_data/{table}/{start_date}-{end_date}",
            "table": table
        }
        payload = json.dumps(params_dict)
        response = requests.post('https://en44bq5e33.execute-api.us-east-1.amazonaws.com/dev/fetch_data', data=payload)
        return response



# check status

job_status = requests.get('https://en44bq5e33.execute-api.us-east-1.amazonaws.com/dev/job_status',
                          data=json.dumps({'job_id': 147841}))
print(job_status.json())
