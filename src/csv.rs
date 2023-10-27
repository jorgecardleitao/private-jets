use std::{collections::HashMap, error::Error};

use serde::Deserialize;

/// Loads a CSV from disk into a HashMap based on the primary key of the type
/// # Error
/// Errors if the file cannot be read
pub fn load<D: for<'de> Deserialize<'de>, PK: Fn(D) -> (String, D)>(
    path: &str,
    map: PK,
) -> Result<HashMap<String, D>, Box<dyn Error>> {
    let data = std::fs::read(path)?;

    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .from_reader(std::io::Cursor::new(data));
    let data = rdr
        .deserialize()
        .into_iter()
        .map(|r| {
            let record: D = r.unwrap();
            record
        })
        .map(map)
        .collect();
    Ok(data)
}
