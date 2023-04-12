import time
import logging
import requests
import json
import datetime
from datetime import timedelta
from databricks.sdk.runtime import dbutils


# noinspection PyTypeChecker
class DataIngest:
    def __init__(self, scope: str, key: str):
        self.scope = scope
        self.key = key
        self.api_key = dbutils.secrets.get(scope=self.scope, key=self.key)
        self.jobs = list()
        self.job_description = dict()
        logging.basicConfig(filename='data_ingestion.log',
                            format='%(asctime)s %(message)s',
                            filemode='w')
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

    def get_data_by_range(self,
                          table: str,
                          start_date: datetime.datetime,
                          end_date: datetime.datetime = datetime.datetime.today() - timedelta(days=1),
                          ):
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
            logging.info(f"Sending request for {table} table between {first_day} and {last_day}")
            self.send_request(first_day, last_day, table)

    def send_request(self, start_date: datetime.datetime, end_date: datetime.datetime, table: str):
        """
        Generate the payload and send the request to the API endpoint
        """
        start_date = start_date.strftime("%m-%d-%Y")
        end_date = end_date.strftime("%m-%d-%Y")
        params_dict = {
            "start_date": start_date,
            "end_date": end_date,
            "api_key": self.api_key,
            "destination_s3_bucket": "allstar-training-mootech",
            "destination_s3_directory": f"raw_data/{table}/{start_date}-{end_date}",
            "table": table
        }
        payload = json.dumps(params_dict)
        while len(self.jobs) >= 2:
            for job_id in self.jobs:
                job_status = requests.get('https://en44bq5e33.execute-api.us-east-1.amazonaws.com/dev/job_status',
                                          data=json.dumps({'job_id': job_id})).json()['execution_status']
                if job_status == 'COMPLETE':
                    logging.info(f"{self.job_description[job_id]} COMPLETED")
                    self.jobs.remove(job_id)
            time.sleep(20)
        response = requests.post('https://en44bq5e33.execute-api.us-east-1.amazonaws.com/dev/fetch_data', data=payload)
        if response.status_code == 200:
            job_id = response.json()['job_id']
            self.job_description[job_id] = f"Job to fetch {table} table between {start_date} and {end_date}"
            logging.info(f"{self.job_description[job_id]} STARTED")
            self.jobs.append(job_id)
        else:
            logging.info(f"{self.job_description[job_id]} FAILED TO START")


# noinspection PyTypeChecker
def main():
    """
    initializes the DataIngest Class and retrieves the data since last load
    for transaction tables and the latest for SCD tables
    """
    # update transactions tables
    data_ingest = DataIngest(scope='mootech-scope', key='mootech-key')
    data_ingest.get_data_by_range(table='clickstream', start_date='<date since last load>')

    data_ingest.get_data_by_range(table='transactions', start_date='<date since last load>')
    # update SCD tables
    day_before_yesterday = datetime.datetime.now() - timedelta(days=2)
    yesterday = datetime.datetime.now() - timedelta(days=1)

    data_ingest.get_data_by_range(table='users',
                                  start_date=day_before_yesterday,
                                  end_date=yesterday,
                                  )

    data_ingest.get_data_by_range(table='products',
                                  start_date=day_before_yesterday,
                                  end_date=yesterday,
                                  )


if __name__ == "__main__":
    main()
