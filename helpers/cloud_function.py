

from google.cloud import iot_v1, bigquery
from math import sqrt, inf


def send_command_to_device(service_account_json, project_id, registry_location, registry_id, deviceID, command):
    
    # Create a client
    client = iot_v1.DeviceManagerClient.from_service_account_file(service_account_json)

    # Initialize request argument(s)
    request = iot_v1.SendCommandToDeviceRequest(
        name=f"projects/{project_id}/locations/{registry_location}/registries/{registry_id}/devices/{deviceID}",
        binary_data=bytes(command,'UTF-8')
    )
    
    # Make the request
    response = client.send_command_to_device(request=request)

    print(response)

    if not response:
        return 200
    else:
        return 500

def query_bq(service_account_json):

    client = bigquery.Client.from_service_account_json(service_account_json)

    query = """
        SELECT DISTINCT t1.vehicle, t1.timestamp, t1.lat, t1.lon FROM `pccreverselogistic.vehicles_positions.positions` AS t1
        WHERE t1.timestamp = (
            SELECT max(timestamp) FROM `pccreverselogistic.vehicles_positions.positions` AS t2
        WHERE t2.vehicle = t1.vehicle
        ) ORDER BY t1.vehicle
    """

    res = client.query(query=query)
    return res
            
def get_nearest_vehicle(res, return_point_lat, return_point_lon):

    nearest_vechicle = 0
    smaller_distance = inf
    
    for row in res:

        distance = sqrt(((row.lat - return_point_lat)**2) + ((row.lon - return_point_lon)**2))

        if (distance <= smaller_distance):
            smaller_distance = distance
            nearest_vechicle = row.vehicle
    
    return nearest_vechicle


# environment variables
service_account_json = "keys/function.json"
project_id = "pccreverselogistic"
registry_location = "europe-west1"
registry_id = "vehicles"
deviceID = "vehicle001"

# # cast data from request
# request_json = request.get_json(silent=True)

# get latitude and longitude
return_point_lat = 119.000
return_point_lon = 39.000

# get data drom bigquery
data = query_bq(service_account_json)
# get nearest vehicle
vehicle = get_nearest_vehicle(data, return_point_lat, return_point_lon)

# create command 
command = f"Vehicle {vehicle} new return to do at location: latitude {return_point_lat} - longitude {return_point_lon}"

print("Trying to send command: ", command)

# send message to vehicle
status = send_command_to_device(service_account_json, project_id, registry_location, registry_id, deviceID, command)




