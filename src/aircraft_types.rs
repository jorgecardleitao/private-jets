use std::{collections::HashMap, error::Error};

use serde::{Deserialize, Serialize};

/// A map of the aircraft type (e.g. `CL30`) to an [`AircraftType`].
pub type AircraftTypes = HashMap<String, AircraftType>;

/// In-memory representation of an aircraft type
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AircraftType {
    /// the type (e.g. `CL30`)
    pub icao: String,
    /// the name of the type (e.g. `Bombardier Challenger 300`)
    pub name: String,
    /// the source that identifies it as a private jet
    pub source: String,
    /// the date of when the source was retrieved
    pub date: String,
}

/// Returns the set of all [`AircraftType`] in `src/types.csv`,
/// corresponding to aircraft types whose primary use is to be a private jet
/// according to the [methodology `M-2`](../methodology.md).
/// # Error
/// Errors if the file cannot be read
pub fn load_private_jet_types() -> Result<AircraftTypes, Box<dyn Error>> {
    super::csv::load("src/types.csv", |a: AircraftType| (a.icao.clone(), a))
}
