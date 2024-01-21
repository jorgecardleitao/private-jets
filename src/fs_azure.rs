use azure_core::{error::HttpError, StatusCode};
use azure_storage::prelude::*;
pub use azure_storage_blobs::prelude::ContainerClient;
use azure_storage_blobs::{container::operations::BlobItem, prelude::ClientBuilder};
use futures::stream::StreamExt;

use crate::fs::BlobStorageProvider;

#[derive(Debug)]
pub enum Error {
    /// Unspecified error interacting with Azure blob storage
    Error(azure_core::Error),
    /// Unauthorized error when interacting with Azure blob storage
    Unauthorized(azure_core::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Unauthorized(e) | Error::Error(e) => e.fmt(f),
        }
    }
}

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

fn get_code(e: &azure_core::Error) -> Option<StatusCode> {
    let a = e.get_ref()?;
    let a = a.downcast_ref::<HttpError>()?;
    Some(a.status())
}

#[async_trait::async_trait]
impl BlobStorageProvider for ContainerClient {
    type Error = Error;

    #[must_use]
    async fn maybe_get(&self, blob_name: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        if exists(self, blob_name).await.map_err(Error::Error)? {
            Ok(Some(
                self.blob_client(blob_name)
                    .get_content()
                    .await
                    .map_err(Error::Error)?,
            ))
        } else {
            Ok(None)
        }
    }

    #[must_use]
    async fn put(&self, blob_name: &str, contents: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        self.blob_client(blob_name)
            .put_block_blob(contents.clone())
            .content_type("text/plain")
            .await
            .map_err(|e| {
                if get_code(&e) == Some(StatusCode::Unauthorized) {
                    Error::Unauthorized(e)
                } else {
                    Error::Error(e)
                }
            })?;
        Ok(contents)
    }
}

pub(crate) async fn cached_call<
    F: Fn() -> G,
    G: futures::Future<Output = Result<Vec<u8>, std::io::Error>>,
>(
    blob_name: &str,
    fetch: F,
    client: Option<&ContainerClient>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let Some(client) = client else {
        return Ok(crate::fs::cached(&blob_name, fetch(), &crate::fs::LocalDisk).await?);
    };

    let result = crate::fs::cached(&blob_name, fetch(), client).await;
    if matches!(
        result,
        Err(crate::fs::Error::Backend(
            crate::fs_azure::Error::Unauthorized(_)
        ))
    ) {
        log::warn!("{blob_name} - Unauthorized - fall back to local disk");
        Ok(crate::fs::cached(&blob_name, fetch(), &crate::fs::LocalDisk).await?)
    } else {
        Ok(result?)
    }
}
