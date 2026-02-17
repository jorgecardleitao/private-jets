pub fn serialize(items: impl Iterator<Item = impl serde::Serialize>) -> Vec<u8> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    for leg in items {
        wtr.serialize(leg).unwrap()
    }
    wtr.into_inner().unwrap()
}

pub fn deserialize<'a, D: serde::de::DeserializeOwned + 'a>(
    data: &'a [u8],
) -> impl Iterator<Item = Result<D, std::io::Error>> + 'a {
    let rdr = csv::ReaderBuilder::new()
        .delimiter(b',')
        .from_reader(std::io::Cursor::new(data));
    rdr.into_deserialize().into_iter().map(|r| {
        let record: D = r?;
        Ok(record)
    })
}
