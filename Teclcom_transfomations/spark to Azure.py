from azure.storage.blob import BlobServiceClient
import os

connection_string = "DefaultEndpointsProtocol=https;AccountName=shivsamb;AccountKey=4HLP3BHRpfqVL4e+9p0zNN+N8JKvRz1lnkA3qu/RZAg35f+B8uE20IiqzuhjeRS0dJcVGGzU/zox+AStRZevNw==;EndpointSuffix=core.windows.net"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

container_name = "silver"
container_client = blob_service_client.get_container_client(container_name)

# Create the container if it doesn't exist
if not container_client.exists():
    container_client.create_container()

# upload files
local_folder_path = r"C:\Users\shree\OneDrive\Desktop\Brain_works-LAPTOP-HAD5JC9F\Telecommunication Project\FINAL_FILES_Initial_Clean\silver\silver_cell_tower_placement"
blob_folder_path = "silver_cell_tower"

for root, dirs, files in os.walk(local_folder_path):
    for file_name in files:
        file_path_on_local = os.path.join(root, file_name)
        file_path_in_blob = os.path.join(blob_folder_path, os.path.relpath(file_path_on_local, local_folder_path))

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path_in_blob)

        # Upload the file
        with open(file_path_on_local, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded {file_path_on_local} to {file_path_in_blob}")
