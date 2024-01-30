use std::{collections::HashMap, error::Error};

use serde::{Deserialize, Serialize};

pub type AircraftTypeConsumptions = HashMap<String, AircraftTypeConsumption>;

/// The in-memory representation of the consumption of an aircraft type
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AircraftTypeConsumption {
    /// the type (e.g. `CL30`)
    pub icao: String,
    /// the consumption in GPH
    pub gph: usize,
    /// the source of the consumption
    pub source: String,
    /// the date of when the source was retrieved
    pub date: String,
}

/// Loads consumption from `src/consumption.csv` into memory has a map `icao: AircraftTypeConsumption`.
/// # Error
/// Errors if the file cannot be read
pub fn load_aircraft_consumption() -> Result<AircraftTypeConsumptions, Box<dyn Error>> {
    super::csv::load("src/consumption.csv", |a: AircraftTypeConsumption| {
        (a.icao.clone(), a)
    })
}
