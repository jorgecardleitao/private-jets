use std::io::Error;

use aws_credential_types::provider::ProvideCredentials;
use aws_sdk_s3::{
    config::Credentials, error::SdkError, operation::get_object::GetObjectError,
    primitives::ByteStream, types::ObjectCannedAcl,
};

use crate::fs::BlobStorageProvider;
pub struct ContainerClient {
    pub client: aws_sdk_s3::Client,
    pub bucket: String,
    can_put: bool,
}

async fn get(client: &ContainerClient, blob_name: &str) -> Result<Option<Vec<u8>>, Error> {
    let maybe_object = client
        .client
        .get_object()
        .bucket(&client.bucket)
        .key(blob_name)
        .send()
        .await;

    let object = match maybe_object {
        Err(err) => match err {
            SdkError::ServiceError(ref e) => {
                if matches!(e.err(), GetObjectError::NoSuchKey(_)) {
                    return Ok(None);
                } else {
                    return Err(Error::other(err));
                }
            }
            _ => return Err(Error::other(err)),
        },
        Ok(x) => x,
    };

    object
        .body
        .collect()
        .await
        .map(|x| Some(x.into_bytes().to_vec()))
        .map_err(Error::other)
}

async fn put(client: &ContainerClient, blob_name: &str, content: Vec<u8>) -> Result<(), Error> {
    let stream = ByteStream::from(content);
    let content_type = blob_name
        .ends_with(".json")
        .then_some("application/json")
        .unwrap_or("text/csv");

    client
        .client
        .put_object()
        .bucket(&client.bucket)
        .key(blob_name)
        .acl(ObjectCannedAcl::PublicRead)
        .body(stream)
        .content_type(content_type)
        .send()
        .await
        .map_err(Error::other)
        .map(|_| ())
}

async fn delete(client: &ContainerClient, blob_name: &str) -> Result<(), Error> {
    client
        .client
        .delete_object()
        .bucket(&client.bucket)
        .key(blob_name)
        .send()
        .await
        .map_err(Error::other)
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
    #[must_use]
    async fn maybe_get(&self, blob_name: &str) -> Result<Option<Vec<u8>>, std::io::Error> {
        get(&self, blob_name).await.map_err(std::io::Error::other)
    }

    #[must_use]
    async fn put(&self, blob_name: &str, contents: Vec<u8>) -> Result<(), std::io::Error> {
        put(&self, blob_name, contents)
            .await
            .map_err(std::io::Error::other)
    }

    #[must_use]
    async fn delete(&self, blob_name: &str) -> Result<(), std::io::Error> {
        delete(&self, blob_name)
            .await
            .map_err(std::io::Error::other)
    }

    #[must_use]
    async fn list(&self, prefix: &str) -> Result<Vec<String>, std::io::Error> {
        Ok(self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .into_paginator()
            .send()
            .try_collect()
            .await
            .map_err(std::io::Error::other)?
            .into_iter()
            .map(|response| {
                response
                    .contents()
                    .iter()
                    .filter_map(|blob| blob.key().map(|x| x.to_string()))
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect())
    }

    fn can_put(&self) -> bool {
        self.can_put
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn get_ok() {
        let client = anonymous_client().await;
        assert!(client
            .maybe_get("leg/v1/status.json")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn get_not_ok() {
        let client = anonymous_client().await;
        assert!(client
            .maybe_get("leg/v1/invalid_basdasdasdasdas.json")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn list_ok() {
        let client = anonymous_client().await;
        assert!(client.list("leg/v1/all/year=2019/").await.unwrap().len() > 0);
    }

    #[tokio::test]
    async fn init_client() {
        let _ = client("".to_string(), "".to_string()).await;
    }
}
