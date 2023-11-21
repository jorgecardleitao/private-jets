use azure_storage::prelude::*;
use azure_storage_blobs::{
    blob::operations::PutBlockBlobResponse, container::operations::BlobItem, prelude::ClientBuilder,
};
use futures::stream::StreamExt;

pub use azure_storage_blobs::prelude::ContainerClient;

/// Lists all blobs in container
pub async fn list(client: ContainerClient) -> Result<Vec<String>, azure_storage::Error> {
    let mut result = vec![];
    let mut blobs = client.list_blobs().into_stream();
    while let Some(response) = blobs.next().await {
        result.extend(
            response?
                .blobs
                .items
                .into_iter()
                .filter_map(|blob| match blob {
                    BlobItem::Blob(blob) => Some(blob.name),
                    BlobItem::BlobPrefix(_) => None,
                }),
        );
    }
    Ok(result)
}

/// Returns whether the blob exists in container
pub async fn exists(
    client: &ContainerClient,
    blob_name: &str,
) -> Result<bool, azure_storage::Error> {
    client.blob_client(blob_name).exists().await
}

/// Puts a blob in container
pub async fn put(
    client: &ContainerClient,
    blob_name: &str,
    content: impl Into<bytes::Bytes>,
) -> Result<PutBlockBlobResponse, azure_storage::Error> {
    client
        .blob_client(blob_name)
        .put_block_blob(content)
        .content_type("text/plain")
        .await
}

/// Gets a blob from container
pub async fn get(
    client: &ContainerClient,
    blob_name: &str,
) -> Result<Vec<u8>, azure_storage::Error> {
    client.blob_client(blob_name).get_content().await
}

/// Initialize write access to the storage
pub fn initialize(
    token: &str,
    account: &str,
    container: &str,
) -> azure_core::Result<ContainerClient> {
    StorageCredentials::sas_token(token)
        .map(|credentials| ClientBuilder::new(account, credentials).container_client(container))
}
