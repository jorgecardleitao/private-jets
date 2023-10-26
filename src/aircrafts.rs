use std::{collections::HashMap, error::Error};

use serde::{Deserialize, Serialize};

pub type Aircrafts = HashMap<String, Aircraft>;

/// The in-memory representation of an aircraft
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Aircraft {
    pub tail_number: String,
    pub model: String,
    pub owner: String,
    pub source: String,
    pub date: String,
}

/// Loads aircrafts from `src/aircrafts.csv` into memory has a map `tail_number: Aircraft`.
/// # Error
/// Errors if the file cannot be read
pub fn load_aircrafts() -> Result<HashMap<String, Aircraft>, Box<dyn Error>> {
    let data = std::fs::read("src/aircrafts.csv")?;

    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .from_reader(std::io::Cursor::new(data));
    let data = rdr
        .deserialize()
        .into_iter()
        .map(|r| {
            let record: Aircraft = r.unwrap();
            record
        })
        .map(|aircraft| (aircraft.tail_number.clone(), aircraft))
        .collect();
    Ok(data)
}
