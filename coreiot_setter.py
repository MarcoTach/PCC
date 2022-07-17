from google.cloud import iot_v1, pubsub_v1, pubsub
import io
from google.api_core import exceptions

# function to create a device registry
def create_registry(service_account_json, project_id, cloud_region, pubsub_topic, registry_id, only_mqtt ):
    """Creates a registry and returns the result. Returns an empty result if
    the registry already exists."""

    # create a device manager client from service account
    client = iot_v1.DeviceManagerClient.from_service_account_json(service_account_json)

    # parent url of the project
    parent = f"projects/{project_id}/locations/{cloud_region}"

    # add parent to topic if not present
    if not pubsub_topic.startswith("projects/"):
        pubsub_topic = "projects/{}/topics/{}".format(project_id, pubsub_topic)

    # enable only mqtt comunications 
    if only_mqtt == True:
        http = iot_v1.HttpConfig()
        http.http_enabled_state = iot_v1.HttpState.HTTP_DISABLED

    # body of the request for create a registry    
    body = {
        "event_notification_configs": [{"pubsub_topic_name": pubsub_topic}],
        "id": registry_id,
        "http_config": 	http
    }

    # try create a registry
    try:
        reg = client.create_device_registry(request={"parent": parent, "device_registry": body})
        print("Created new registry: ",  reg.id)
        return
    except Exception as e:
        return e
        

# function to create a pub/sub topic
def create_topic(service_account_json, project_id, topic_id):
    # create PublisherClient
    publisher = pubsub_v1.PublisherClient.from_service_account_file(service_account_json)


    topic_path = publisher.topic_path(project_id, topic_id)

    try:
        top = publisher.create_topic(request={"name": topic_path})
        print("Topic {} created....".format(topic_id))
        return top
    except Exception as e:
        return e
    

def create_subscription(service_account_json, project_id, topic_id, subscription_id):

    publisher = pubsub_v1.PublisherClient.from_service_account_file(service_account_json)
    subscriber = pubsub_v1.SubscriberClient.from_service_account_file(service_account_json)
    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    try:
        sub = subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
        print("Subcription {} created....".format(subscription_id))
        return sub
    except Exception as e:
        return e





def create_device(service_account_json, project_id, cloud_region, registry_id, device_id, certificate_file):
    # create an istance of DeviceManagerClient
    client = iot_v1.DeviceManagerClient.from_service_account_json(service_account_json)
    # create parent url
    parent = f"projects/{project_id}/locations/{cloud_region}/registries/{registry_id}"
    # read the public key
    with io.open(certificate_file) as f:
        certificate = f.read()
    # create device template
    device_template = {
        "id": device_id,
        "credentials": [
            {
                "public_key": {
                    "format": iot_v1.PublicKeyFormat.RSA_PEM,
                    "key": certificate,
                }
            }
        ],
    }
    try:    
        dev = client.create_device(request={"parent": parent, "device": device_template})    
        print("Device {} created....".format(dev.id))
        return dev
    except Exception as e:
        return e 
    


