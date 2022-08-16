from google.oauth2 import service_account
from google.cloud import pubsub_v1

# TODO(developer)
project_id = 'poseidon-sandbox-2c57'
topic_id = 'test-sr-test-msgs-pub'
json_key = 'D:/Sreenu/MyWork/gcp/sa_keys/poseidon-sandbox-2c57/test-account-sr.json'

credentials = service_account.Credentials.from_service_account_file(json_key)

publisher = pubsub_v1.PublisherClient(credentials=credentials)
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

for n in range(1, 2):
    data_str = f"Order number {n}"
    # Data must be a bytestring
    data = data_str.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data, user='Sreenu', purpose='Test')
    print(future.result())

print(f"Published messages to {topic_path}.")