use std::fmt::Display;

use aws_credential_types::provider::ProvideCredentials;
use aws_sdk_s3::{
    config::Credentials, error::SdkError, operation::head_object::HeadObjectError,
    primitives::ByteStream, types::ObjectCannedAcl,
};

use crate::fs::BlobStorageProvider;

#[derive(Clone, Debug)]
pub struct Error(String);

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error(value)
    }
}

pub struct ContainerClient {
    pub client: aws_sdk_s3::Client,
    pub bucket: String,
    can_put: bool,
}

/// Returns whether the blob exists
async fn exists(client: &ContainerClient, blob_name: &str) -> Result<bool, Error> {
    let head_object_output = client
        .client
        .head_object()
        .bucket(&client.bucket)
        .key(blob_name)
        .send()
        .await;

    match head_object_output {
        Ok(_) => Ok(true),
        Err(err) => match &err {
            SdkError::ServiceError(e) => {
                if matches!(e.err(), HeadObjectError::NotFound(_)) {
                    Ok(false)
                } else {
                    Err(format!("{err:?}").into())
                }
            }
            _ => Err(format!("{err:?}").into()),
        },
    }
}

async fn get(client: &ContainerClient, blob_name: &str) -> Result<Vec<u8>, Error> {
    let object = client
        .client
        .get_object()
        .bucket(&client.bucket)
        .key(blob_name)
        .send()
        .await
        .map_err(|e| Error::from(format!("{e:?}")))?;

    object
        .body
        .collect()
        .await
        .map(|x| x.into_bytes().to_vec())
        .map_err(|e| format!("{e:?}").into())
}

async fn put(client: &ContainerClient, blob_name: &str, content: Vec<u8>) -> Result<(), Error> {
    let stream = ByteStream::from(content);

    client
        .client
        .put_object()
        .bucket(&client.bucket)
        .key(blob_name)
        .acl(ObjectCannedAcl::PublicRead)
        .body(stream)
        .send()
        .await
        .map_err(|e| Error::from(format!("{e:?}")))
        .map(|_| ())
}

#[derive(Debug)]
struct Provider {
    access_key: String,
    secret_access_key: String,
}

impl ProvideCredentials for Provider {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        let access_key = self.access_key.clone();
        let secret_access_key = self.secret_access_key.clone();
        aws_credential_types::provider::future::ProvideCredentials::new(async {
            Ok(Credentials::new(
                access_key,
                secret_access_key,
                None,
                None,
                "example",
            ))
        })
    }
}

/// Initialize a [`ContainerClient`] access key and secret access key
pub async fn client(access_key: String, secret_access_key: String) -> ContainerClient {
    let provider = Provider {
        access_key,
        secret_access_key,
    };

    let config = aws_config::ConfigLoader::default()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .region("fra1")
        .endpoint_url("https://fra1.digitaloceanspaces.com")
        .credentials_provider(provider)
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&config);

    ContainerClient {
        client,
        bucket: "private-jets".to_string(),
        can_put: true,
    }
}

/// Initialize an anonymous [`ContainerClient`]
pub async fn anonymous_client() -> ContainerClient {
    let config = aws_config::ConfigLoader::default()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .region("fra1")
        .endpoint_url("https://fra1.digitaloceanspaces.com")
        .no_credentials()
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&config);

    ContainerClient {
        client,
        bucket: "private-jets".to_string(),
        can_put: false,
    }
}

#[async_trait::async_trait]
impl BlobStorageProvider for ContainerClient {
    type Error = Error;

    #[must_use]
    async fn maybe_get(&self, blob_name: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        if exists(self, blob_name).await? {
            Ok(Some(get(&self, blob_name).await?))
        } else {
            Ok(None)
        }
    }

    #[must_use]
    async fn put(&self, blob_name: &str, contents: Vec<u8>) -> Result<(), Self::Error> {
        put(&self, blob_name, contents).await
    }

    fn can_put(&self) -> bool {
        self.can_put
    }
}

/// * read from remote
/// * if not found and can't write to remote => read disk and write to disk
/// * if not found and can write to remote => fetch and write
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
