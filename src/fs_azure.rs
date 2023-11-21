use azure_storage::prelude::*;
pub use azure_storage_blobs::prelude::ContainerClient;
use azure_storage_blobs::{container::operations::BlobItem, prelude::ClientBuilder};
use futures::stream::StreamExt;

use crate::fs::BlobStorageProvider;

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
async fn exists(client: &ContainerClient, blob_name: &str) -> Result<bool, azure_storage::Error> {
    client.blob_client(blob_name).exists().await
}

/// Initialize a [`ContainerClient`] using SAS token
pub fn initialize_sas(
    token: &str,
    account: &str,
    container: &str,
) -> azure_core::Result<ContainerClient> {
    StorageCredentials::sas_token(token)
        .map(|credentials| ClientBuilder::new(account, credentials).container_client(container))
}

/// Initialize an anonymous [`ContainerClient`]
pub fn initialize_anonymous(account: &str, container: &str) -> ContainerClient {
    ClientBuilder::new(account, StorageCredentials::anonymous()).container_client(container)
}

pub struct AzureContainer<'a>(pub &'a ContainerClient);

#[async_trait::async_trait]
impl BlobStorageProvider for ContainerClient {
    type Error = azure_core::Error;

    #[must_use]
    async fn maybe_get(&self, blob_name: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        if exists(self, blob_name).await? {
            Ok(Some(self.blob_client(blob_name).get_content().await?))
        } else {
            Ok(None)
        }
    }

    #[must_use]
    async fn put(&self, blob_name: &str, contents: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        self.blob_client(blob_name)
            .put_block_blob(contents.clone())
            .content_type("text/plain")
            .await?;
        Ok(contents)
    }
}
