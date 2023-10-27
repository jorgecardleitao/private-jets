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
    super::csv::load("src/aircrafts.csv", |a: Aircraft| {
        (a.tail_number.clone(), a)
    })
}
