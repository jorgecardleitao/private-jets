use std::path::{Path, PathBuf};

use async_trait::async_trait;

static ROOT: &'static str = "database/";

/// An object that can be used to get and put blobs.
#[async_trait]
pub trait BlobStorageProvider {
    async fn maybe_get(&self, blob_name: &str) -> Result<Option<Vec<u8>>, std::io::Error>;
    async fn put(&self, blob_name: &str, contents: Vec<u8>) -> Result<(), std::io::Error>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, std::io::Error>;
    async fn delete(&self, blob_name: &str) -> Result<(), std::io::Error>;

    fn can_put(&self) -> bool;
}

/// A [`BlobStorageProvider`] for local disk
pub struct LocalDisk;

#[async_trait]
impl BlobStorageProvider for LocalDisk {
    #[must_use]
    async fn maybe_get(&self, blob_name: &str) -> Result<Option<Vec<u8>>, std::io::Error> {
        let path = PathBuf::from(ROOT).join(Path::new(blob_name));
        if path.try_exists()? {
            Ok(Some(std::fs::read(path)?))
        } else {
            Ok(None)
        }
    }

    #[must_use]
    async fn put(&self, blob_name: &str, contents: Vec<u8>) -> Result<(), std::io::Error> {
        let path = PathBuf::from(ROOT).join(Path::new(blob_name));
        let mut dir = path.clone();
        dir.pop();
        std::fs::create_dir_all(dir)?;
        std::fs::write(path, &contents)?;
        Ok(())
    }

    #[must_use]
    async fn list(&self, _prefix: &str) -> Result<Vec<String>, std::io::Error> {
        todo!()
    }

    #[must_use]
    async fn delete(&self, _prefix: &str) -> Result<(), std::io::Error> {
        todo!()
    }

    fn can_put(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheAction {
    ReadFetchWrite,
    ReadFetch,
    FetchWrite,
}

impl CacheAction {
    pub fn from_date(date: &time::Date) -> Self {
        let now = time::OffsetDateTime::now_utc().date();
        (date >= &now)
            .then_some(Self::ReadFetch)
            .unwrap_or(Self::ReadFetchWrite)
    }
}

/// Tries to retrive `blob_name` from `provider`. If it does not exist,
/// it calls `fetch` and writes the result into `provider`.
/// Returns the data in `blob_name` from `provider`.
/// # Implementation
/// This function is idempotent but not pure.
pub async fn cached<E, F>(
    blob_name: &str,
    fetch: F,
    provider: &dyn BlobStorageProvider,
    action: CacheAction,
) -> Result<Vec<u8>, std::io::Error>
where
    E: std::error::Error + Send + Sync + 'static,
    F: futures::Future<Output = Result<Vec<u8>, E>>,
{
    match action {
        CacheAction::FetchWrite => miss(blob_name, fetch, provider, action).await,
        _ => {
            log::info!("Fetch {blob_name}");
            if let Some(data) = provider.maybe_get(blob_name).await? {
                log::info!("{blob_name} - cache hit");
                Ok(data)
            } else {
                miss(blob_name, fetch, provider, action).await
            }
        }
    }
}

/// Writes the result of `fetch` into `provider`.
/// Returns the result of fetch.
/// # Implementation
/// This function is idempotent and pure.
pub async fn miss<E, F>(
    blob_name: &str,
    fetch: F,
    provider: &dyn BlobStorageProvider,
    action: CacheAction,
) -> Result<Vec<u8>, std::io::Error>
where
    E: std::error::Error + Send + Sync + 'static,
    F: futures::Future<Output = Result<Vec<u8>, E>>,
{
    log::info!("{blob_name} - cache miss");
    let contents = fetch.await.map_err(std::io::Error::other)?;
    if action == CacheAction::ReadFetch || !provider.can_put() {
        log::info!("{blob_name} - cache do not write");
        return Ok(contents);
    };
    provider.put(blob_name, contents.clone()).await?;
    log::info!("{blob_name} - cache write");
    Ok(contents)
}

/// * read from remote
/// * if not found and can't write to remote => read disk and write to disk
/// * if not found and can write to remote => fetch and write
pub(crate) async fn cached_call<F: futures::Future<Output = Result<Vec<u8>, std::io::Error>>>(
    blob_name: &str,
    fetch: F,
    client: Option<&dyn BlobStorageProvider>,
    action: crate::fs::CacheAction,
) -> Result<Vec<u8>, std::io::Error> {
    let client = client.unwrap_or(&crate::fs::LocalDisk);

    let Some(data) = client.maybe_get(blob_name).await? else {
        if !client.can_put() {
            return crate::fs::cached(&blob_name, fetch, &crate::fs::LocalDisk, action).await;
        } else {
            return crate::fs::cached(&blob_name, fetch, client, action).await;
        };
    };
    Ok(data)
}
