use async_trait::async_trait;

/// An object that can be used to get and put blobs.
#[async_trait]
pub trait BlobStorageProvider {
    type Error: std::error::Error + Send;
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

#[derive(Debug)]
pub enum Error<F: std::error::Error + Send, E: std::error::Error + Send> {
    /// An error originating from trying to read from source
    Fetch(F),
    /// An error originating from trying to read or write data from/to backend
    Backend(E),
}

impl<F: std::error::Error + Send, E: std::error::Error + Send> std::error::Error for Error<F, E> {}

impl<F: std::error::Error + Send, E: std::error::Error + Send> std::fmt::Display for Error<F, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fetch(e) => std::fmt::Display::fmt(&e, f),
            Self::Backend(e) => std::fmt::Display::fmt(&e, f),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheAction {
    ReadFetchWrite,
    ReadFetch,
}

/// Tries to retrive `blob_name` from `provider`. If it does not exist,
/// it calls `fetch` and writes the result into `provider`.
/// Returns the data in `blob_name` from `provider`.
/// # Implementation
/// This function is idempotent but not pure.
pub async fn cached<E, P, F>(
    blob_name: &str,
    fetch: F,
    provider: &P,
    action: CacheAction,
) -> Result<Vec<u8>, Error<E, P::Error>>
where
    E: std::error::Error + Send,
    F: futures::Future<Output = Result<Vec<u8>, E>>,
    P: BlobStorageProvider,
{
    log::info!("Fetch {blob_name}");
    if let Some(data) = provider
        .maybe_get(blob_name)
        .await
        .map_err(|e| Error::Backend(e))?
    {
        log::info!("{blob_name} - cache hit");
        Ok(data)
    } else {
        log::info!("{blob_name} - cache miss");
        let contents = fetch.await.map_err(|e| Error::Fetch(e))?;
        if action == CacheAction::ReadFetch {
            log::info!("{blob_name} - cache do not write");
            return Ok(contents);
        };
        let data = provider
            .put(blob_name, contents)
            .await
            .map_err(|e| Error::Backend(e))?;
        log::info!("{blob_name} - cache write");
        Ok(data)
    }
}
