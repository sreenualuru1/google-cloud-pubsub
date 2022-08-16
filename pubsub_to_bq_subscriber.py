import json
import pandas as pd

from google.oauth2 import service_account
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

json_key = 'D:/Sreenu/MyWork/gcp/sa_keys/poseidon-sandbox-2c57/test-account-sr.json'

project_id = 'poseidon-sandbox-2c57'
subscription_id = 'sales-data-pull-sub'
# Number of seconds the subscriber should listen for messages
timeout = 10.0

# BQ table details
bq_table_name = 'test_sr.test_sales_data'
bq_table_schema = [{'name': 'OrderID', 'type': 'STRING'},
                   {'name': 'Item', 'type': 'STRING'},
                   {'name': 'UnitPrice', 'type': 'FLOAT'},
                   {'name': 'Qty', 'type': 'INTEGER'},
                   {'name': 'TotalPrice', 'type': 'FLOAT'},
                   {'name': 'CustID', 'type': 'STRING'},
                   {'name': 'Name', 'type': 'STRING'},
                   {'name': 'Contact', 'type': 'INTEGER'}]

# data processing
def process_data(message: pubsub_v1.subscriber.message.Message) -> None:
    message.ack()
    print(message)

    # decode json string
    json_decode = message.data.decode('utf-8')
    json_data = json.loads(json_decode)

    new_df = pd.read_json(json.dumps(json_data))
    # print(new_df.head(n=10))
    new_df.to_gbq(destination_table=str(bq_table_name),
                  project_id=project_id,
                  chunksize=100,
                  if_exists='append',
                  table_schema=bq_table_schema,
                  credentials=credentials)
    print('Data pushed to BQ table successfully')


credentials = service_account.Credentials.from_service_account_file(json_key)
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
subscription_path = subscriber.subscription_path(
    project=project_id, subscription=subscription_id)
streaming_pull = subscriber.subscribe(
    subscription=subscription_path, callback=process_data)
print('Listining for messages on {subscription}...\n'.format(
    subscription=subscription_path))

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull.result(timeout=timeout)
    except TimeoutError:
        streaming_pull.cancel()  # Trigger the shutdown.
        streaming_pull.result()  # Block until the shutdown is complete.
