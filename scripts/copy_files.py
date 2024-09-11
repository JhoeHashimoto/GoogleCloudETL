import functions_framework

import os
from google.cloud import storage
from google.oauth2 import service_account

def get_env_variable(var_name):
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Environment variable {var_name} not set.")
    return value


def copy_files_within_bucket(source_bucket_name, destination_bucket_name, 
                             source_folder, destination_folder):

    client = storage.Client()
    source_bucket = client.bucket(source_bucket_name)
    destination_bucket = client.bucket(destination_bucket_name)

    destination_blobs = set(blob.name for blob in destination_bucket.list_blobs(prefix=destination_folder))

    blobs = source_bucket.list_blobs(prefix=source_folder)


    for blob in blobs:

        if blob.name.endswith("/"):
            continue
        destination_blob_name = blob.name.replace(source_folder, destination_folder, 1)

        if destination_blob_name in destination_blobs:
            print(f'Arquivo {blob.name} já existe no destino e não será copiado.')
            continue
        
        new_blob = destination_bucket.copy_blob(blob, destination_bucket, destination_blob_name)
        
        print(f'Arquivo {blob.name} copiado para {new_blob.name}')


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def copy_files_trigger(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    source_bucket = get_env_variable('source_bucket_name')
    destination_bucket_name = get_env_variable('destination_bucket_name')
    source_folder = ''
    destination_folder = get_env_variable('destination_folder')

    copy_files_within_bucket(source_bucket, destination_bucket_name, source_folder, destination_folder)