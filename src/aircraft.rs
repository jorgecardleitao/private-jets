//! Contains the implementation to extract the database of all aircrafts available in ADS-B exchange
//! The database contains "current" status.
use std::error::Error;
use std::{collections::HashMap, sync::Arc};

use async_recursion::async_recursion;
use reqwest;
use serde::{Deserialize, Serialize};
use serde_json;
use time::Date;

use crate::csv;
use crate::fs::BlobStorageProvider;
use crate::CountryIcaoRanges;

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
    /// The country in ISO 3166 of the aircraft
    pub country: Option<Arc<str>>,
}

fn file_path(date: Date) -> String {
    format!("aircraft/db/date={date}/data.csv")
}

fn url(prefix: &str) -> String {
    format!("https://globe.adsbexchange.com/db-current/{prefix}.js")
}

/// Returns the current aircrafts from adsbexchange.com
/// on a specific prefix of ICAO.
async fn db_current(
    prefix: String,
) -> Result<(String, HashMap<String, Vec<Option<String>>>), String> {
    let data = reqwest::get(url(&prefix))
        .await
        .map_err(|e| e.to_string())?
        .bytes()
        .await
        .map_err(|e| e.to_string())?;

    Ok((
        prefix,
        serde_json::from_slice(&data).map_err(|e| e.to_string())?,
    ))
}

#[async_recursion]
async fn children<'a: 'async_recursion>(
    entries: &mut HashMap<String, Vec<Option<String>>>,
) -> Result<Vec<(String, HashMap<String, Vec<Option<String>>>)>, String> {
    let Some(entries) = entries.remove("children") else {
        return Ok(Default::default());
    };

    let mut entries = futures::future::try_join_all(
        entries
            .into_iter()
            .map(|x| x.unwrap())
            .map(|x| db_current(x)),
    )
    .await
    .map_err(|e| e.to_string())?;

    // recurse over all children
    let mut _children =
        futures::future::try_join_all(entries.iter_mut().map(|entry| children(&mut entry.1)))
            .await?;

    entries.extend(_children.into_iter().flatten());
    Ok(entries)
}

/// Returns [`Aircrafts`] known in [ADS-B exchange](https://globe.adsbexchange.com) as of now.
/// It returns ~0.5m aircrafts
/// # Implementation
/// This function is not pure: the result depends on adsbexchange.com's current state.
async fn extract_aircrafts() -> Result<Vec<Aircraft>, Box<dyn Error>> {
    let prefixes = (b'A'..=b'F').chain(b'0'..b'9');
    let prefixes = prefixes.map(|x| std::str::from_utf8(&[x]).unwrap().to_string());
    extract_aircrafts_prefix(prefixes).await
}

async fn extract_aircrafts_prefix(
    prefixes: impl Iterator<Item = String>,
) -> Result<Vec<Aircraft>, Box<dyn Error>> {
    let country_ranges = CountryIcaoRanges::new();

    let mut entries = futures::future::try_join_all(prefixes.map(|x| db_current(x))).await?;

    let mut _children =
        futures::future::try_join_all(entries.iter_mut().map(|entry| children(&mut entry.1)))
            .await?;

    entries.extend(_children.into_iter().flatten());

    Ok(entries
        .into_iter()
        .fold(vec![], |mut acc, (prefix, values)| {
            let items = values
                .into_iter()
                .map(|(k, v)| (format!("{prefix}{k}"), v))
                .filter_map(|(icao_number, mut data)| {
                    let tail_number = std::mem::take(&mut data[0])?;
                    let type_designator = std::mem::take(&mut data[1])?;
                    let model = std::mem::take(&mut data[3])?;
                    let country = country_ranges.country(&icao_number).unwrap();

                    Some(Aircraft {
                        icao_number: icao_number.to_ascii_lowercase().into(),
                        tail_number,
                        type_designator,
                        model,
                        country: country.cloned(),
                    })
                });
            acc.extend(items);
            acc
        }))
}

async fn load(
    aircraft: Vec<Aircraft>,
    blob_name: &str,
    client: &dyn BlobStorageProvider,
) -> Result<(), Box<dyn Error>> {
    let contents = csv::serialize(aircraft.into_iter());
    client.put(blob_name, contents).await?;
    Ok(())
}

pub async fn etl_aircrafts(client: &dyn BlobStorageProvider) -> Result<(), Box<dyn Error>> {
    let now = time::OffsetDateTime::now_utc().date();
    let blob_name = file_path(now);
    let aircraft = extract_aircrafts().await?;
    load(aircraft, &blob_name, client).await
}

pub async fn read(date: Date, client: &dyn BlobStorageProvider) -> Result<Aircrafts, String> {
    let path = file_path(date);
    let data = client.maybe_get(&path).await.map_err(|e| e.to_string())?;
    let data = data.ok_or_else(|| format!("File {path} does not exist"))?;

    Ok(super::csv::deserialize(&data)
        .map(|x: Aircraft| (x.tail_number.clone(), x))
        .collect())
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn work() {
        assert!(db_current("A0".to_string()).await.unwrap().1.len() > 20000);

        assert!(
            extract_aircrafts_prefix(["A00".to_string()].into_iter())
                .await
                .unwrap()
                .len()
                > 1000
        );

        //assert!(extract_aircrafts().await.unwrap().len() > 400000);
    }

    #[tokio::test]
    async fn load_works() {
        load(vec![], "tst.csv", &crate::LocalDisk).await.unwrap();
    }
}
