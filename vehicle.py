# imports
import os, jwt, ssl
import datetime, time
import paho.mqtt.client as mqtt
import json

# create token from Google Key
def create_jwt(project_id, private_key_file, algorithm):
    """Creates a JWT (https://jwt.io) to establish an MQTT connection.
    Args:
     project_id: The cloud project ID this device belongs to
     private_key_file: A path to a file containing either an RSA256 or
             ES256 private key.
     algorithm: The encryption algorithm to use. Either 'RS256' or 'ES256'
    Returns:
        A JWT generated from the given project_id and private key, which
        expires in 20 minutes. After 20 minutes, your client will be
        disconnected, and a new JWT will have to be generated.
    Raises:
        ValueError: If the private_key_file does not contain a known key.
    """

    token = {
        # The time that the token was issued at
        "iat": datetime.datetime.now(tz=datetime.timezone.utc),
        # The time the token expires.
        "exp": datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(minutes=24),
        # The audience field should always be set to the GCP project id.
        "aud": project_id,
    }
    # Read the private key file.
    with open(private_key_file, "r") as f:
        private_key = f.read()
    print(
        "Creating JWT using {} from private key file {}".format(
            algorithm, private_key_file
        )
    )
    return jwt.encode(token, private_key, algorithm=algorithm)

# Events callbacks
def error_str(rc):
    """Convert a Paho error to a human readable string."""
    return "{}: {}".format(rc, mqtt.error_string(rc))

def on_connect(unused_client, unused_userdata, unused_flags, rc):

    """Callback for when a device connects.
    RC values could be:
    0: Connection successful 
    1: Connection refused - incorrect protocol version 
    2: Connection refused - invalid client identifier 
    3: Connection refused - server unavailable 
    4: Connection refused - bad username or password 
    5: Connection refused - not authorised 
    6-255: Currently unused.
    mqtt.connack_string translates that values"""

    print("on_connect", mqtt.connack_string(rc))

    # After a successful connect, reset backoff time and stop backing off.
    global should_backoff
    global minimum_backoff_time
    should_backoff = False
    minimum_backoff_time = 1

def on_disconnect(unused_client, unused_userdata, rc):
    """Paho callback for when a device disconnects."""
    print("on_disconnect", error_str(rc))

    # Since a disconnect occurred, the next loop iteration will wait with
    # exponential backoff.
    global should_backoff
    should_backoff = True

def on_publish(unused_client, unused_userdata, unused_mid):
    """Paho callback when a message is sent to the broker."""
    #print("Sent message with ID {}".format(unused_mid))

def on_message(unused_client, unused_userdata, message):
    """Callback when the device receives a message on a subscription.
    Called when a message has been received on a topic that the client subscribes 
    to and the message does not match an existing topic filter callback. 
    Use message_callback_add() to define a callback that will be called for 
    specific topic filters. on_message will serve as fallback when none matched."""
    
    # decode UTF-8 payload
    payload = str(message.payload.decode("utf-8"))
    print(
        "Received message '{}' on topic '{}' with Qos {}".format(
            payload, message.topic, str(message.qos)
        )
    )

# create client
def get_client(
    project_id,
    cloud_region,
    registry_id,
    device_id,
    private_key_file,
    algorithm,
    ca_certs,
    mqtt_bridge_hostname,
    mqtt_bridge_port,
):
    """Create our MQTT client. The client_id is a unique string that identifies
    this device. For Google Cloud IoT Core, it must be in the format below."""
    client_id = "projects/{}/locations/{}/registries/{}/devices/{}".format(
        project_id, cloud_region, registry_id, device_id
    )
    print("Device client_id is '{}'".format(client_id))

    # Create Client, do not retain offline informations (clean_session=True)
    client = mqtt.Client(client_id=client_id, clean_session=True)

    # With Google Cloud IoT Core, the username field is ignored, and the
    # password field is used to transmit a JWT to authorize the device.
    client.username_pw_set(
        username="unused", password=create_jwt(project_id, private_key_file, algorithm)
    )

    # Google IoT Core needs SSL/TLS support.
    # The ca_certs is supplied by Google. It is needed to verify the public key of google domain mqtt.googleapis.com
    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    # Set paho callbacks
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    # Connect to the Google MQTT bridge in a SYNC way
    client.connect(mqtt_bridge_hostname, mqtt_bridge_port)

    # The topic that the device will receive commands on.
    mqtt_command_topic = "/devices/{}/commands/#".format(device_id)

    # Subscribe to the commands topic, QoS 1 enables message acknowledgement.
    print("Subscribing to {}".format(mqtt_command_topic))
    client.subscribe(mqtt_command_topic, qos=1)

    return client


