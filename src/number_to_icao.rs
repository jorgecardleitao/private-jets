use std::{collections::HashMap, error::Error};

use reqwest;
use serde_json;

static CACHE_FILE_PATH: &'static str = "database/number_to_icao.json";
static SOURCE: &'static str = "https://globe.adsbexchange.com/db-1697242032/regIcao.js";

/// Returns a map between tail number (e.g. "OYTWM": "45D2ED")
/// Caches to disk the first time it is executed
pub fn number_to_icao() -> Result<HashMap<String, String>, Box<dyn Error>> {
    if !std::path::Path::new(CACHE_FILE_PATH).exists() {
        let req = reqwest::blocking::get(SOURCE)?;
        let data = req.text()?;
        std::fs::write(CACHE_FILE_PATH, data)?;
    }

    let data = std::fs::read(CACHE_FILE_PATH)?;
    Ok(serde_json::from_slice(&data)?)
}
