from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from dotenv import dotenv_values
import os

# TODO(developer)
# project_id = "your-project-id"
# subscription_id = "your-subscription-id"
# Number of seconds the subscriber should listen for messages


# read .env file for envirnoment variables
config = dotenv_values(".env")

timeout = 120.0

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["GOOGLE_APPLICATION_CREDENTIALS_WEBCLIENT"]

subscription = "vehicle_realtime_positions"

subscriber = pubsub_v1.SubscriberClient()

# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(config["GOOGLE_CLOUD_PROJECT_ID"], subscription)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    data = message.data.decode("UTF-8")
    print(f"Received {data}.")
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.