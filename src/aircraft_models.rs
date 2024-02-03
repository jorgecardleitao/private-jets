use std::{collections::HashMap, error::Error};

use serde::{Deserialize, Serialize};

/// A map of the aircraft model (e.g. `BEECH 400 Beechjet`) to an [`AircraftModel`].
pub type AircraftModels = HashMap<String, AircraftModel>;

/// In-memory representation of an aircraft model
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AircraftModel {
    /// the model (e.g. `BEECH 400 Beechjet`)
    pub model: String,
    /// the consumption in gallons per hour
    pub gph: u32,
    /// the source that identifies it as a private jet
    pub source: String,
    /// the date of when the source was retrieved
    pub date: String,
}

/// Returns the set of all [`AircraftModel`] in `src/models.csv`,
/// corresponding to aircraft types whose primary use is to be a private jet
/// according to the [methodology `M-2`](../methodology.md).
/// # Error
/// Errors if the file cannot be read
pub fn load_private_jet_models() -> Result<AircraftModels, Box<dyn Error>> {
    super::csv::load("src/models.csv", |a: AircraftModel| (a.model.clone(), a))
}
