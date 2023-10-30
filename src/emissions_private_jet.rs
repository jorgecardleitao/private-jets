use std::{collections::HashMap, error::Error};

use crate::{Fact, Position};

#[derive(Debug, serde::Deserialize, Clone)]
pub struct Specification {
    #[serde(rename = "type")]
    pub type_: String,
    pub max_fuel: f64,
    pub max_range: f64,
    pub source: String,
    pub date: String,
}

// see https://www.iata.org/en/services/statistics/intelligence/co2-connect/iata-co2-connect-passenger-calculator/calculator-faq/
const FUEL_TO_CO2: f64 = 3.16; // kg CO2 / kg fuel

/// Loads aircrafts from `src/specifications.csv` into memory has a map `icao: Specification`.
/// # Error
/// Errors if the file cannot be read
pub fn load_specification() -> Result<HashMap<String, Specification>, Box<dyn Error>> {
    super::csv::load("src/specifications.csv", |s: Specification| {
        (s.type_.clone(), s)
    })
}

/// Compute emissions of a private jet by taking into account:
/// 1. The distance between positions
/// 2. The fuel consumption of the aircraft in ideal conditions (cruise)
/// 3.
pub fn emissions_private_jet(
    type_: &str,
    from: Position,
    to: Position,
    specifications: &HashMap<String, Specification>,
) -> Fact<usize> {
    let spec = specifications.get(type_).unwrap();
    let efficiency = spec.max_fuel / spec.max_range;

    let distance = super::distance(from.pos(), to.pos());

    let cruise_emissions = efficiency * distance * FUEL_TO_CO2;

    // this number was computed as follows:
    // * used "1.A.3.a Aviation - Annex 5 - LTO emissions calculator 2019_2020" to obtain
    //   LTO emissions for aircraft F2TH in airport LFPG (Paris)
    // * convert CO2 emissions + NOX emissions to eCO2 using https://www.epa.gov/energy/greenhouse-gas-equivalencies-calculator
    // 1.0064*10^3 (kg CO2) + 2.7202 (kg NOX) = 1.7 (mT eCO2)
    // TODO: migrate calculation performed in the Annex 5 for all airplanes and airports to here
    let lto_emissions = 1700.0;

    Fact {
        claim: (cruise_emissions + lto_emissions) as usize,
        source: format!(
            "Aircraft {type_} at cruise speed has a fuel efficiency of {efficiency:.2} kg/km ({}, {}),
that correspond to {cruise_emissions:.2} kg CO2e for the distance of {distance:.2} km (using jet fuel to CO2 conversion used by IATA)
plus emissions during LTO of ~1700 CO2e kg (based on EPA's [1.A.3.a Aviation 2 LTO emissions calculator 2019](https://www.eea.europa.eu/publications/emep-eea-guidebook-2019/part-b-sectoral-guidance-chapters/1-energy/1-a-combustion/1-a-3-a-aviation-1-annex5-LTO/view) 
and EPA's [Greenhouse Gas Equivalencies Calculator](https://www.epa.gov/energy/greenhouse-gas-equivalencies-calculator))",
            spec.source, spec.date,
        ),
        date: "".to_string(),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn emissions() {
        let spec = load_specification().unwrap();
        let r = emissions_private_jet(
            "F2TH",
            Position::Grounded(36665.76, 55.739834, 9.138823),
            Position::Grounded(43346.08, 41.849808, 12.559995),
            &spec,
        );
        assert_eq!(r.claim, 6743)
    }
}
