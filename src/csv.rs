use std::{collections::HashMap, error::Error, hash::Hash};

use serde::Deserialize;

/// Loads a CSV from disk into a HashMap based on the primary key of the type
/// # Error
/// Errors if the file cannot be read
pub(crate) fn load<H: Hash + Eq, D: for<'de> Deserialize<'de>, PK: Fn(D) -> (H, D)>(
    path: &str,
    map: PK,
) -> Result<HashMap<H, D>, Box<dyn Error>> {
    let data = std::fs::read(path)?;

    let data = deserialize(&data).map(map).collect();
    Ok(data)
}

pub fn serialize(items: impl Iterator<Item = impl serde::Serialize>) -> Vec<u8> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    for leg in items {
        wtr.serialize(leg).unwrap()
    }
    wtr.into_inner().unwrap()
}

pub fn deserialize<'a, D: serde::de::DeserializeOwned + 'a>(
    data: &'a [u8],
) -> impl Iterator<Item = D> + 'a {
    let rdr = csv::ReaderBuilder::new()
        .delimiter(b',')
        .from_reader(std::io::Cursor::new(data));
    rdr.into_deserialize().into_iter().map(|r| {
        let record: D = r.unwrap();
        record
    })
}
