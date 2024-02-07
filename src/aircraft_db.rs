use std::error::Error;
/// Contains the implementation to extract the database of all aircrafts available in ADS-B exchange
use std::{collections::HashMap, sync::Arc};

use async_recursion::async_recursion;
use reqwest;
use serde::{Deserialize, Serialize};
use serde_json;

use crate::{fs, fs_s3};

/// [`HashMap`] between tail number (e.g. "OY-TWM") and an [`Aircraft`]
pub type Aircrafts = HashMap<String, Aircraft>;

/// An in-memory representation of an aircraft data
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Aircraft {
    /// The ICAO number of the aircraft (e.g. `459CD3`)
    pub icao_number: Arc<str>,
    /// The tail number of the aircraft (e.g. `OY-GFS`)
    pub tail_number: String,
    /// The ICAO number of the aicraft model (e.g. `F2TH`)
    pub type_designator: String,
    /// The model
    pub model: String,
}

static DATABASE: &'static str = "db-20231106";

fn cache_file_path(prefix: &str) -> String {
    format!("{DATABASE}/{prefix}.json")
}

fn url(prefix: &str) -> String {
    format!("https://globe.adsbexchange.com/{DATABASE}/{prefix}.js")
}

async fn aircrafts(prefix: &str) -> Result<Vec<u8>, reqwest::Error> {
    Ok(reqwest::get(url(prefix))
        .await?
        .bytes()
        .await
        .map(|x| x.into())?)
}

/// Returns a map between tail number (e.g. "OYTWM": "45D2ED")
/// Caches to disk or remote storage the first time it is executed
async fn aircrafts_prefixed(
    prefix: String,
    client: Option<&fs_s3::ContainerClient>,
) -> Result<(String, HashMap<String, Vec<Option<String>>>), String> {
    let blob_name = cache_file_path(&prefix);
    let fetch = aircrafts(&prefix);

    let data = match client {
        Some(client) => fs::cached(&blob_name, fetch, client, fs::CacheAction::ReadFetchWrite)
            .await
            .map_err(|e| e.to_string())?,
        None => fs::cached(
            &blob_name,
            fetch,
            &fs::LocalDisk,
            fs::CacheAction::ReadFetchWrite,
        )
        .await
        .map_err(|e| e.to_string())?,
    };

    Ok((
        prefix,
        serde_json::from_slice(&data).map_err(|e| e.to_string())?,
    ))
}

#[async_recursion]
async fn children<'a: 'async_recursion>(
    entries: &mut HashMap<String, Vec<Option<String>>>,
    client: Option<&'a fs_s3::ContainerClient>,
) -> Result<Vec<(String, HashMap<String, Vec<Option<String>>>)>, String> {
    let Some(entries) = entries.remove("children") else {
        return Ok(Default::default());
    };

    let mut entries = futures::future::try_join_all(
        entries
            .into_iter()
            .map(|x| x.unwrap())
            .map(|x| aircrafts_prefixed(x, client)),
    )
    .await
    .map_err(|e| e.to_string())?;

    // recurse over all children
    let mut _children = futures::future::try_join_all(
        entries
            .iter_mut()
            .map(|entry| children(&mut entry.1, client)),
    )
    .await?;

    entries.extend(_children.into_iter().flatten());
    Ok(entries)
}

/// Returns [`Aircrafts`] known in [ADS-B exchange](https://globe.adsbexchange.com) as of 2023-11-06.
/// It returns ~0.5m aircrafts
/// # Implementation
/// This function is idempotent but not pure: it caches every https request either to disk or remote storage
/// to not penalize adsbexchange.com
pub async fn load_aircrafts(
    client: Option<&fs_s3::ContainerClient>,
) -> Result<Aircrafts, Box<dyn Error>> {
    let prefixes = (b'A'..=b'F').chain(b'0'..b'9');
    let prefixes = prefixes.map(|x| std::str::from_utf8(&[x]).unwrap().to_string());

    let mut entries =
        futures::future::try_join_all(prefixes.map(|x| aircrafts_prefixed(x, client))).await?;

    let mut _children = futures::future::try_join_all(
        entries
            .iter_mut()
            .map(|entry| children(&mut entry.1, client)),
    )
    .await?;

    entries.extend(_children.into_iter().flatten());

    Ok(entries
        .into_iter()
        .fold(HashMap::default(), |mut acc, (prefix, values)| {
            let items = values
                .into_iter()
                .map(|(k, v)| (format!("{prefix}{k}"), v))
                .filter_map(|(icao_number, mut data)| {
                    let tail_number = std::mem::take(&mut data[0])?;
                    let type_designator = std::mem::take(&mut data[1])?;
                    let model = std::mem::take(&mut data[3])?;
                    Some((
                        tail_number.clone(),
                        Aircraft {
                            icao_number: icao_number.to_ascii_lowercase().into(),
                            tail_number,
                            type_designator,
                            model,
                        },
                    ))
                });
            acc.extend(items);
            acc
        }))
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn work() {
        assert_eq!(
            aircrafts_prefixed("A0".to_string(), None)
                .await
                .unwrap()
                .1
                .len(),
            24465
        );
        // although important, this is an expensive call to run on every test => only run ad-hoc
        //assert_eq!(aircrafts().unwrap().len(), 463747);
    }
}
