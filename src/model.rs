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
