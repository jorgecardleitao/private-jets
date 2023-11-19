/// Contains the implementation to extract the database of all aircrafts available in ADS-B exchange
use std::collections::HashMap;
use std::error::Error;

use reqwest;
use serde::{Deserialize, Serialize};
use serde_json;

/// [`HashMap`] between tail number (e.g. "OY-TWM") and an [`Aircraft`]
pub type Aircrafts = HashMap<String, Aircraft>;

/// An in-memory representation of an aircraft data
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Aircraft {
    /// The ICAO number of the aircraft (e.g. `459CD3`)
    pub icao_number: String,
    /// The tail number of the aircraft (e.g. `OY-GFS`)
    pub tail_number: String,
    /// The ICAO number of the aicraft model (e.g. `F2TH`)
    pub model: String,
}

static DATABASE: &'static str = "db-20231106";
static DIRECTORY: &'static str = "database";

fn cache_file_path(prefix: &str) -> String {
    format!("{DIRECTORY}/{DATABASE}/{prefix}.json")
}
fn source(prefix: &str) -> String {
    format!("https://globe.adsbexchange.com/{DATABASE}/{prefix}.js")
}

/// Returns  a map between tail number (e.g. "OYTWM": "45D2ED")
/// Caches to disk the first time it is executed
fn aircrafts_prefixed(
    prefix: &str,
) -> Result<HashMap<String, Vec<Option<String>>>, Box<dyn Error>> {
    let path = &cache_file_path(prefix);
    if !std::path::Path::new(path).exists() {
        let source = &source(prefix);
        let req = reqwest::blocking::get(source)?;
        let data = req.text()?;
        std::fs::create_dir_all(format!("{DIRECTORY}/{DATABASE}"))?;
        std::fs::write(path, data)?;
    }

    let data = std::fs::read(path)?;
    Ok(serde_json::from_slice(&data)?)
}

fn children(
    entries: &mut HashMap<String, Vec<Option<String>>>,
) -> Result<Vec<(String, HashMap<String, Vec<Option<String>>>)>, Box<dyn Error>> {
    let Some(entries) = entries.remove("children") else {
        return Ok(Default::default());
    };

    let mut entries = entries
        .into_iter()
        .map(|x| x.unwrap())
        .map(|x| aircrafts_prefixed(&x).map(|r| (x, r)))
        .collect::<Result<Vec<_>, _>>()?;

    // recurse over all children
    let children = entries
        .iter_mut()
        .map(|(_, ref mut r)| children(r))
        .collect::<Result<Vec<_>, _>>()?;

    entries.extend(children.into_iter().flatten());
    Ok(entries)
}

/// Returns [`Aircrafts`] known in [ADS-B exchange](https://globe.adsbexchange.com) as of 2023-11-06.
/// It returns ~0.5m aircrafts
/// # Implementation
/// This function is idempotent but not pure: it caches every https request to disk to not penalize adsbexchange.com
pub fn load_aircrafts() -> Result<Aircrafts, Box<dyn Error>> {
    let prefixes = (b'A'..=b'F').chain(b'0'..b'9');
    let prefixes = prefixes.map(|x| std::str::from_utf8(&[x]).unwrap().to_string());

    let mut entries = prefixes
        .map(|x| aircrafts_prefixed(&x).map(|r| (x, r)))
        .collect::<Result<Vec<_>, _>>()?;

    let children = entries
        .iter_mut()
        .map(|(_, ref mut r)| children(r))
        .collect::<Result<Vec<_>, _>>()?;

    entries.extend(children.into_iter().flatten());

    Ok(entries
        .into_iter()
        .fold(HashMap::default(), |mut acc, (prefix, values)| {
            let items = values
                .into_iter()
                .map(|(k, v)| (format!("{prefix}{k}"), v))
                .filter_map(|(icao_number, mut data)| {
                    let tail_number = std::mem::take(&mut data[0])?;
                    let model = std::mem::take(&mut data[1])?;
                    Some((
                        tail_number.clone(),
                        Aircraft {
                            icao_number: icao_number.to_ascii_lowercase(),
                            tail_number,
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

    #[test]
    fn work() {
        assert_eq!(aircrafts_prefixed("A0").unwrap().len(), 24465);
        // although important, this is an expensive call to run on every test => only run ad-hoc
        //assert_eq!(aircrafts().unwrap().len(), 463747);
    }
}
