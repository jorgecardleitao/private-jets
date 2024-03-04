use std::io::Cursor;

use crate::fs;

#[derive(Debug, serde::Deserialize, Clone)]
pub struct Airport {
    pub name: String,
    pub latitude_deg: f64,
    pub longitude_deg: f64,
    #[serde(rename = "type")]
    pub type_: String,
}

async fn airports() -> Result<Vec<u8>, reqwest::Error> {
    let url = "https://raw.githubusercontent.com/davidmegginson/ourairports-data/main/airports.csv";
    Ok(reqwest::get(url).await?.bytes().await.map(|x| x.into())?)
}

/// Returns a list of airports
/// # Implementation
/// Data is cached on disk the first time it is executed
pub async fn airports_cached() -> Result<Vec<Airport>, Box<dyn std::error::Error>> {
    let data = fs::cached(
        "database/airports.csv",
        airports(),
        &fs::LocalDisk,
        fs::CacheAction::ReadFetchWrite,
    )
    .await?;

    let mut rdr = csv::Reader::from_reader(Cursor::new(data));
    let data = rdr
        .deserialize()
        .into_iter()
        .map(|r| {
            let record: Airport = r.unwrap();
            record
        })
        .filter(|airport| airport.type_ == "medium_airport" || airport.type_ == "large_airport")
        .collect::<Vec<_>>();

    Ok(data)
}

/// Returns the closest [`Airport`] from `pos`.
pub fn closest(pos: (f64, f64), airports: &[Airport]) -> Airport {
    airports
        .iter()
        .fold((airports[0].clone(), f64::MAX), |mut acc, airport| {
            let distance = super::distance(pos, (airport.latitude_deg, airport.longitude_deg));
            if distance < acc.1 {
                acc.0 = airport.clone();
                acc.1 = distance;
            }
            acc
        })
        .0
}
