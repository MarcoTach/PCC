from bisect import bisect
from google.cloud import bigquery, exceptions
from google.cloud.bigquery.enums import EntityTypes

def create_dataset_and_table(service_account_json, project_id, dataset, table):

    # init client
    client = bigquery.Client.from_service_account_json(service_account_json)
    # create dataset string
    dataset_string = bigquery.Dataset(dataset_ref=f"{project_id}.{dataset}")

    

    # create dataset (must exists before creating a table)
    try:
        createdDataset = client.create_dataset(dataset=dataset_string, exists_ok=True)

        entries = list(createdDataset.access_entries)

        needupdate = True

        for ace in entries:
            if ace.user_by_email == 'functions@pccreverselogistic.iam.gserviceaccount.com':
                if ace.role == 'READER':
                    needupdate = False

        if needupdate == True:
            entity_id = 'functions@pccreverselogistic.iam.gserviceaccount.com'
            entity_type = EntityTypes.USER_BY_EMAIL
            entries.append(
                bigquery.AccessEntry(
                    role="READER",
                    entity_id=entity_id, 
                    entity_type=entity_type
                    )
                )
            createdDataset.access_entries = entries

            # update permissions
            createdDatasetPermission = client.update_dataset(createdDataset, ["access_entries"])

            full_dataset_id = "{}.{}".format(createdDatasetPermission.project, createdDatasetPermission.dataset_id)

            print("Updated dataset '{}' with modified user permissions for {}".format(full_dataset_id, entity_id))
        print(f"Bigquery dataset: {dataset} ok.")
    except Exception as e:
        print(e.message)

    # creating the new schema
    schema = [
        bigquery.SchemaField(name="vehicle", field_type="INTEGER", description="Vehicle ID"),
        bigquery.SchemaField(name="timestamp", field_type="TIMESTAMP", description="Publication Time"),
        bigquery.SchemaField(name="lat", field_type="FLOAT", description="Latitude"),
        bigquery.SchemaField(name="lon", field_type="FLOAT", description="Longitude"),
    ]

    # defining the new table
    table_req = bigquery.Table(table_ref=f"{project_id}.{createdDataset.dataset_id}.{table}", schema=schema)

    try:
        table = client.create_table(table=table_req, exists_ok=True)
        print(f"Bigquery table: {table.table_id} ok...")
        return table
    except Exception as e:
        return e

    