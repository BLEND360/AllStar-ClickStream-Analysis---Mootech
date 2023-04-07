import requests
import json

# data_fetch

params_dict = {
"start_date": "01-01-2023",
"end_date": "01-02-2023",
"api_key": "0449e790-2b4a-4887-8716-151fce2d6cdf",
"destination_s3_bucket": "allstar-training-mootech",
"destination_s3_directory": "raw_data/users/",
"table": "users"
}
payload = json.dumps(params_dict)
response = requests.post('https://en44bq5e33.execute-api.us-east-1.amazonaws.com/dev/fetch_data', data=payload)
print(response.text)

# check status

job_status = requests.get('https://en44bq5e33.execute-api.us-east-1.amazonaws.com/dev/job_status', data=json.dumps({'job_id': 147841}))
print(job_status.json())

# from blend360_all_star_clickstream_api.datafetch import DataFetch
# data_fetch = DataFetch(secret_scope='mootech-scope', key_name= 'mootech-key')
# data_fetch.fetchData(start_date = datetime.date(2022, 1, 1), end_date = datetime.date(2023, 1, 1),
#                      destination_bucket= "allstar-training-mootech", destination_directory = "tables/clickstream").json()