from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

# Path to your service account key file
service_account_path = "key.json"

# Define your project ID, topic ID, and subscription ID
project_id = "myownproject241124"
topic_id = "myownproj-tp11"
subscription_id = "subfor-tp11"

# Create a subscriber client using the service account key
subscriber = pubsub_v1.SubscriberClient.from_service_account_file(service_account_path)
topic_path = f"projects/{project_id}/topics/{topic_id}"
subscription_path = f"projects/{project_id}/subscriptions/{subscription_id}"

# Create the subscription (if it doesn't already exist)
try:
    subscriber.create_subscription(name=subscription_path, topic=topic_path)
    print(f"Subscription {subscription_id} created.")
except AlreadyExists:
    print(f"Subscription {subscription_id} already exists.")

# Pull messages from the subscription
def callback(message):
    print(f"Received message: {message.data.decode('utf-8')}")
    message.ack()  # Acknowledge the message to remove it from the queue

# Listen to the subscription
print(f"Listening for messages on {subscription_path}...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    # Block the main thread indefinitely while waiting for messages
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("Stopped listening for messages.")
