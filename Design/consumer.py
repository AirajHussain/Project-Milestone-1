from google.cloud import pubsub_v1


import glob      
import json      
import os        


files = glob.glob("*.json")


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]


project_id = "coral-broker-484817-n5"


topic_name = ""


subscription_id = "labelsTopic-sub"


subscriber = pubsub_v1.SubscriberClient()


subscription_path = subscriber.subscription_path(project_id, subscription_id)


print(f"Listening for messages on {subscription_path}..\n")


def callback(message: pubsub_v1.subscriber.message.Message) -> None:


    # Decode the message data from bytes and parse it as JSON
    message_data = json.loads(message.data.decode("utf-8"))

    # Print only the values from the consumed message
    print("Consumed values:", list(message_data.values()))

    # Acknowledge the message so it is not redelivered
    message.ack()



with subscriber:
   
    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=callback
    )
    try:
        
        streaming_pull_future.result()
    except KeyboardInterrupt:
      
        streaming_pull_future.cancel()
