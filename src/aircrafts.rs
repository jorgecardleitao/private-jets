use std::{collections::HashMap, error::Error};

use serde::{Deserialize, Serialize};

pub type AircraftOwners = HashMap<String, AircraftOwner>;

/// The in-memory representation of an aircraft owner
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AircraftOwner {
    pub tail_number: String,
    pub owner: String,
    pub source: String,
    pub date: String,
}

/// Loads owners from `src/owners.csv` into memory has a map `tail_number: KnownOwner`.
/// # Error
/// Errors if the file cannot be read
pub fn load_aircraft_owners() -> Result<AircraftOwners, Box<dyn Error>> {
    super::csv::load("src/owners.csv", |a: AircraftOwner| {
        (a.tail_number.clone(), a)
    })
}
