use serde::de::DeserializeOwned;

use crate::fs::BlobStorageProvider;

pub async fn get_csv<D: DeserializeOwned>(
    key: &str,
    client: &dyn BlobStorageProvider,
) -> Result<Vec<D>, std::io::Error> {
    let content = client.maybe_get(key).await?.expect("File to be present");

    super::csv::deserialize::<D>(&content).collect()
}
