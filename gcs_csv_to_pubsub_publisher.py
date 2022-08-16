import pandas as pd

from google.oauth2 import service_account
from google.cloud import pubsub_v1


json_key = 'D:/Sreenu/MyWork/gcp/sa_keys/poseidon-sandbox-2c57/test-account-sr.json'
project_id = 'poseidon-sandbox-2c57'
topic_id = 'test-sr-sales-data-pub'
bucket_name = 'test-bucket-sr'
prefix = 'test-sales-data'
gcs_file_path = 'gs://test-bucket-sr/test-sales-data/sales_data_20220811.csv'

# read csv
df = pd.read_csv(filepath_or_buffer=gcs_file_path,
                 storage_options={'token': json_key})
# print(df.head(n=10))

# convert to json string and encode
json_str = df.to_json(orient='records')
json_encode = json_str.encode('utf-8')
# print(json_encode)

credentials = service_account.Credentials.from_service_account_file(
    filename=json_key)
publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(project=project_id, topic=topic_id)
future = publisher.publish(topic=topic_path, data=json_encode)
print(future.result())
print('Data published to topic: {pubsub_topic}'.format(
    pubsub_topic=topic_path))
