use azure_storage::prelude::*;
use azure_storage_blobs::prelude::ClientBuilder;
pub use azure_storage_blobs::prelude::ContainerClient as _ContainerClient;

use crate::fs::BlobStorageProvider;

pub struct ContainerClient {
    pub client: _ContainerClient,
    can_put: bool,
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
) -> Result<Vec<u8>, std::io::Error> {
    let Some(client) = client else {
        return Ok(
            crate::fs::cached(&blob_name, fetch, &crate::fs::LocalDisk, action)
                .await
                .map_err(std::io::Error::other)?,
        );
    };

    let Some(data) = client
        .maybe_get(blob_name)
        .await
        .map_err(std::io::Error::other)?
    else {
        return Ok(if !client.can_put() {
            crate::fs::cached(&blob_name, fetch, &crate::fs::LocalDisk, action)
                .await
                .map_err(std::io::Error::other)?
        } else {
            crate::fs::cached(&blob_name, fetch, client, action)
                .await
                .map_err(std::io::Error::other)?
        });
    };
    Ok(data)
}
