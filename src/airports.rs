use std::io::Cursor;

#[derive(Debug, serde::Deserialize, Clone)]
pub struct Airport {
    pub name: String,
    pub latitude_deg: f64,
    pub longitude_deg: f64,
    #[serde(rename = "type")]
    pub type_: String,
}

async fn airports() -> Result<String, Box<dyn std::error::Error>> {
    let url = "https://raw.githubusercontent.com/davidmegginson/ourairports-data/main/airports.csv";

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    Ok(client.get(url).send().await?.text().await?)
}

/// Returns a list of airports
pub async fn airports_cached() -> Result<Vec<Airport>, Box<dyn std::error::Error>> {
    let file_path = "database/airports.csv";
    if !std::path::Path::new(&file_path).exists() {
        let data = airports().await?;
        std::fs::write(&file_path, data)?;
    }

    let data = std::fs::read(file_path)?;

    let mut rdr = csv::Reader::from_reader(Cursor::new(data));
    let data = rdr
        .deserialize()
        .into_iter()
        .map(|r| {
            let record: Airport = r.unwrap();
            record
        })
        .filter(|airport| airport.type_ != "heliport")
        .collect::<Vec<_>>();

    Ok(data)
}

/// Returns the closest [`Airport`] from `pos`.
pub fn closest(pos: (f64, f64), airports: &[Airport]) -> Airport {
    airports
        .iter()
        .fold((airports[0].clone(), f64::MAX), |mut acc, airport| {
            let distance = super::distance(pos, (airport.latitude_deg, airport.longitude_deg));
            if distance < acc.1 {
                acc.0 = airport.clone();
                acc.1 = distance;
            }
            acc
        })
        .0
}
