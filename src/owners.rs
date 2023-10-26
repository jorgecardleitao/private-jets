use std::{collections::HashMap, error::Error};

use crate::{Company, Fact};

pub type Owners = HashMap<String, Company>;

/// Loads owners json into memory
pub fn load_owners() -> Result<HashMap<String, Company>, Box<dyn Error>> {
    let data = std::fs::read("src/owners.json")?;
    let value: HashMap<String, Fact<String>> = serde_json::from_slice(&data)?;

    Ok(value
        .into_iter()
        .map(|(name, v)| (name.clone(), Company { name, statement: v }))
        .collect())
}
