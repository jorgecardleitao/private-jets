use azure_storage::prelude::*;
pub use azure_storage_blobs::prelude::ContainerClient as _ContainerClient;
use azure_storage_blobs::{container::operations::BlobItem, prelude::ClientBuilder};
use futures::stream::StreamExt;

use crate::fs::BlobStorageProvider;

pub struct ContainerClient {
    client: _ContainerClient,
    can_put: bool,
}

/// Lists all blobs in container
pub async fn list(client: ContainerClient) -> Result<Vec<String>, azure_storage::Error> {
    let mut result = vec![];
    let mut blobs = client.client.list_blobs().into_stream();
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
    client.client.blob_client(blob_name).exists().await
}

/// Initialize a [`ContainerClient`] using SAS token
pub fn initialize_sas(
    token: &str,
    account: &str,
    container: &str,
) -> azure_core::Result<ContainerClient> {
    StorageCredentials::sas_token(token)
        .map(|credentials| ClientBuilder::new(account, credentials).container_client(container))
        .map(|client| ContainerClient {
            client,
            can_put: true,
        })
}

/// Initialize an anonymous [`ContainerClient`]
pub fn initialize_anonymous(account: &str, container: &str) -> ContainerClient {
    let client =
        ClientBuilder::new(account, StorageCredentials::anonymous()).container_client(container);

    ContainerClient {
        client,
        can_put: false,
    }
}

#[async_trait::async_trait]
impl BlobStorageProvider for ContainerClient {
    type Error = azure_core::Error;

    #[must_use]
    async fn maybe_get(&self, blob_name: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        if exists(self, blob_name).await? {
            Ok(Some(
                self.client.blob_client(blob_name).get_content().await?,
            ))
        } else {
            Ok(None)
        }
    }

    #[must_use]
    async fn put(&self, blob_name: &str, contents: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        self.client
            .blob_client(blob_name)
            .put_block_blob(contents.clone())
            .content_type("text/plain")
            .await?;
        Ok(contents)
    }

    fn can_put(&self) -> bool {
        self.can_put
    }
}

/// * read from azure
/// * if not found and can't write to azure => read disk and write to disk
/// * if not found and can write to azure => fetch and write
pub(crate) async fn cached_call<F: futures::Future<Output = Result<Vec<u8>, std::io::Error>>>(
    blob_name: &str,
    fetch: F,
    action: crate::fs::CacheAction,
    client: Option<&ContainerClient>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let Some(client) = client else {
        return Ok(crate::fs::cached(&blob_name, fetch, &crate::fs::LocalDisk, action).await?);
    };

    let Some(data) = client.maybe_get(blob_name).await? else {
        return Ok(if !client.can_put() {
            crate::fs::cached(&blob_name, fetch, &crate::fs::LocalDisk, action).await?
        } else {
            crate::fs::cached(&blob_name, fetch, client, action).await?
        });
    };
    Ok(data)
}
