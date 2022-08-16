import json
from google.oauth2 import service_account
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

# TODO(developer)
project_id = 'poseidon-sandbox-2c57'
subscription_id = 'test-sr-test-msgs-sub'
# Number of seconds the subscriber should listen for messages
timeout = 10.0

json_key = 'D:/Sreenu/MyWork/gcp/sa_keys/poseidon-sandbox-2c57/test-account-sr.json'
credentials = service_account.Credentials.from_service_account_file(json_key)

subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f'Received {message}')
    data = message.data.decode('utf-8')
    print(f'Data Received: {data}')
    if message.attributes:
        print("Attributes Below:")
        for key in message.attributes:
            value = message.attributes.get(key)
            print(f"{key}: {value}")
    message.nack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.