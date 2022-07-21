import enum
from logging import exception
from numpy import greater
from coreiot_setter import create_device, create_registry, create_topic, create_subscription
from google.cloud import iot_v1
import time, json
from google.api_core import exceptions
from vehicle import get_client
from dataflow import create_job_from_template
from bigquery import create_dataset_and_table

# setting variables
device_id = 'vehicle001'
service_account_json = 'keys/coreiot.json'
project_id = 'pccreverselogistic'
cloud_region = 'europe-west1' 
device_pubsub_topic = 'vehicle_position'
subscription_id = "vehicle_realtime_positions"
registry_id = 'vehicles' 
only_mqtt = True
algorithm = "RS256"
mqtt_bridge_hostname = 'mqtt.googleapis.com'
mqtt_bridge_port = 8883
ca_certs = "p_keys/ca/roots.cer"
certificate_file = 'p_keys/RSA/rsa_public.pem'
private_key_file = "p_keys/RSA/rsa_private.pem"
bqCollection="vehicles_positions"
bqOutputTable="positions"

print("-------- Registry ---------")
# create registry
registry = create_registry(service_account_json, project_id, cloud_region, device_pubsub_topic, registry_id, only_mqtt)
# excepts AlreadyExists errors
if type(registry) == exceptions.AlreadyExists:
    print(registry.message, "... going on with next step")

print("-------- Topic ---------")
# create topic
topic = create_topic(service_account_json, project_id, device_pubsub_topic)
# check if topic already exists
if type(topic) == exceptions.AlreadyExists:
    print("The topic {} already exists.... going on with next step".format(device_pubsub_topic))

print("-------- Subscription ---")
sub = create_subscription(service_account_json, project_id, device_pubsub_topic, subscription_id)
# check if subscription already exists
if type(sub) == exceptions.AlreadyExists:
    print("The subscription {} already exists.... going on with next step".format(subscription_id))

print("-------- Device ---------")
# create device
device = create_device(service_account_json, project_id, cloud_region, registry_id, device_id, certificate_file)
# check if device already exists
if type(device) == exceptions.AlreadyExists:
    print("Device already exists... going on with next step")
elif type(device) == exceptions.InvalidArgument:
    print(device)
elif type(device) == iot_v1.Device:
    print("Device created: ", device_id)

print("-------- BigQuery ---------")
tab = create_dataset_and_table(service_account_json, project_id=project_id, dataset=bqCollection, table=bqOutputTable)
if type(tab) == exceptions.Conflict:
    print(f"Table {bqCollection}.{bqOutputTable} already exists .... going on with next step")


print("-------- Dataflow Job ---------")
# create dataflow job or check if it is running
df = create_job_from_template(service_account_json, project_id, device_pubsub_topic, bqCollection, bqOutputTable )
if type(df) == exceptions.AlreadyExists:
    print(df.message, "... going on with client")


print("-------- Execution ---------")
# create mqtt client
client = get_client(
    project_id=project_id,
    cloud_region=cloud_region,
    registry_id=registry_id,
    device_id=device_id,
    private_key_file=private_key_file,
    algorithm=algorithm,
    ca_certs=ca_certs,
    mqtt_bridge_hostname=mqtt_bridge_hostname,
    mqtt_bridge_port=mqtt_bridge_port
)

# qos = 1 -> is fire and forget (client receives no acknoledge), on_publish is called also if the message isn't sent (loop not called)
# qos = 1 means at least 1 (client receives an ack),
# qos = 2 exactly one (slower)

# number of vehicle for data
number_of_vehicles = 20
vehicles = [a for a in range(1, number_of_vehicles + 1)]

# rows to read in the file 
row = 1

# master topic where publish messages
master_topic = '/devices/{}/events'.format(device_id)

client.loop_start()
while True:
    data = []
    for v in vehicles:
        with open("vehicles_data/{}.txt".format(v)) as fp:
            for i, line in enumerate(fp):
                if i == row:
                    data.append(line.strip())
                    
    for idx, data in enumerate(data):
        topic = master_topic + "/" + str(idx + 1)
        # data is like 17,2008-02-02 13:55:02,116.07255,38
        splitted_data = str(data).split(",")
        payload = {
            "vehicle": splitted_data[0],
            "timestamp": splitted_data[1],
            "lat": splitted_data[2],
            "lon": splitted_data[3]
        }
        client.publish(topic, payload=json.dumps(payload).encode(encoding="UTF-8"), qos=0, retain=False)
    time.sleep(20)
    row = row + 1

client.loop_stop()

#client.loop_forever()

