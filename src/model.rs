use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Fact<R> {
    pub claim: R,
    pub source: String,
    pub date: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Company {
    pub name: String,
    pub statement: Fact<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Event {
    pub tail_number: String,
    pub owner: Fact<Company>,
    pub date: String,
    pub from_airport: String,
    pub to_airport: String,
    pub two_way: bool,
    pub commercial_emissions_kg: Fact<usize>,
    pub emissions_kg: Fact<usize>,
    pub source: String,
    pub source_date: String,
}

#[derive(Serialize)]
pub struct Context {
    pub event: Event,
    pub dane_emissions_kg: Fact<usize>,
    pub dane_years: String,
}
