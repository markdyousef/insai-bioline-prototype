from google.cloud import pubsub_v1
import os

PROJECT_ID = os.getenv('PROJECT_ID')
TOPIC = os.getenv('PUBSUB_TOPIC')
SUBSCRIPTION = os.getenv('PUBSUB_SUBSCRIPTION')

def get_subscription():
    client = pubsub_v1.SubscriberClient()
    topic = 'projects/{}/topics/{}'.format(PROJECT_ID, TOPIC)
    subscription_name = 'projects/{}/subscriptions/{}'.format(PROJECT_ID, SUBSCRIPTION)
    
    # get subscription
    subscription = client.subscribe(subscription_name)

    # TODO: if subscription doesn't exists create
    # try:
    #     subscription = client.create_subscription(subscription_name, topic)
    # except Exception, e:
    #     print(e)

    return subscription