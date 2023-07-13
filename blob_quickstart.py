from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobSasPermissions, BlobServiceClient, BlobClient, UserDelegationKey, generate_blob_sas

import datetime
import uuid


def request_user_delegation_key(blob_service_client: BlobServiceClient) -> UserDelegationKey:
    # Get a user delegation key that's valid for 1 day
    delegation_key_start_time = datetime.datetime.now(datetime.timezone.utc)
    delegation_key_expiry_time = delegation_key_start_time + datetime.timedelta(days=1)

    user_delegation_key = blob_service_client.get_user_delegation_key(
        key_start_time=delegation_key_start_time,
        key_expiry_time=delegation_key_expiry_time
    )

    return user_delegation_key


def create_user_delegation_sas_blob(blob_client: BlobClient, user_delegation_key: UserDelegationKey) -> str:
    # Create a SAS token that's valid for one day, as an example
    start_time = datetime.datetime.now(datetime.timezone.utc)
    expiry_time = start_time + datetime.timedelta(days=1)

    return generate_blob_sas(
        account_name=str(blob_client.account_name),
        container_name=blob_client.container_name,
        blob_name=blob_client.blob_name,
        user_delegation_key=user_delegation_key,
        permission=BlobSasPermissions(read=True),
        expiry=expiry_time,
        start=start_time
    )


try:
    print("Azure Blob Storage Python quickstart sample")

    account_url = "https://juvodev.blob.core.windows.net"
    default_credential = DefaultAzureCredential()

    container_name = "file-uploads"
    local_file_name = "blob-example.docx"

    # Create a blob client using the local file name as the name for the blob
    blob_service_client = BlobServiceClient(account_url, credential=default_credential)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=str(uuid.uuid4()))


    print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
    # Upload the created file
    with open(file=local_file_name, mode="rb") as data:
        blob_client.upload_blob(data)

    user_delegation_key = request_user_delegation_key(blob_service_client)
    sas_token = create_user_delegation_sas_blob(blob_client, user_delegation_key)
    # The SAS token string can be appended to the resource URL with a ? delimiter
    # or passed as the credential argument to the client constructor
    sas_url = f"{blob_client.url}?{sas_token}"
    print(sas_url)

except Exception as ex:
    print('Exception:')
    print(ex)

