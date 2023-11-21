use async_trait::async_trait;

/// An object that can be used to get and put blobs.
#[async_trait]
pub trait BlobStorageProvider {
    type Error: std::error::Error;
    async fn maybe_get(&self, blob_name: &str) -> Result<Option<Vec<u8>>, Self::Error>;
    async fn put(&self, blob_name: &str, contents: Vec<u8>) -> Result<Vec<u8>, Self::Error>;
}

/// A [`BlobStorageProvider`] for local disk
pub struct LocalDisk;

#[async_trait]
impl BlobStorageProvider for LocalDisk {
    type Error = std::io::Error;

    #[must_use]
    async fn maybe_get(&self, blob_name: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        if std::path::Path::new(blob_name).try_exists()? {
            Ok(Some(std::fs::read(blob_name)?))
        } else {
            Ok(None)
        }
    }

    #[must_use]
    async fn put(&self, blob_name: &str, contents: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        let mut dir: std::path::PathBuf = blob_name.into();
        dir.pop();
        std::fs::create_dir_all(dir)?;
        std::fs::write(blob_name, &contents)?;
        Ok(contents)
    }
}

/// Tries to retrive `blob_name` from `provider`. If it does not exist,
/// it calls `fetch` and writes the result into `provider`.
/// Returns the data in `blob_name` from `provider`.
/// # Implementation
/// This function is idempotent but not pure.
pub async fn cached<'a, P, F>(
    blob_name: &str,
    fetch: F,
    provider: &P,
) -> Result<Vec<u8>, Box<dyn std::error::Error + 'a>>
where
    F: futures::Future<Output = Result<Vec<u8>, Box<dyn std::error::Error>>>,
    P: BlobStorageProvider,
    P::Error: 'a,
{
    log::info!("Fetch {blob_name}");
    if let Some(data) = provider.maybe_get(blob_name).await? {
        log::info!("{blob_name} - cache hit");
        Ok(data)
    } else {
        log::info!("{blob_name} - cache miss");
        let contents = fetch.await?;
        Ok(provider.put(blob_name, contents).await?)
    }
}
