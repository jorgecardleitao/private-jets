/// Prints how much of the 2023 dataset has been computed.
use std::collections::HashSet;

use itertools::Itertools;

use flights::Aircraft;

async fn private_jets(
    client: Option<&flights::fs_azure::ContainerClient>,
) -> Result<Vec<Aircraft>, Box<dyn std::error::Error>> {
    // load datasets to memory
    let aircrafts = flights::load_aircrafts(client).await?;
    let models = flights::load_private_jet_models()?;

    Ok(aircrafts
        .into_iter()
        // its primary use is to be a private jet
        .filter_map(|(_, a)| models.contains_key(&a.model).then_some(a))
        .collect())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let months = (2023..2024)
        .cartesian_product(1..=12u8)
        .map(|(year, month)| {
            time::Date::from_calendar_date(year, time::Month::try_from(month).unwrap(), 1)
                .expect("day 1 never errors")
        })
        .collect::<Vec<_>>();

    let client = flights::fs_azure::initialize_anonymous("privatejets", "data");

    let existing = flights::existing_months_positions(&months, &client).await?;

    let private_jets = private_jets(Some(&client)).await?;
    println!("jets    : {}", private_jets.len());
    let required = private_jets
        .into_iter()
        .map(|a| a.icao_number.clone())
        .cartesian_product(months.into_iter())
        .collect::<HashSet<_>>();

    let computed = required.intersection(&existing).count();
    println!("required: {}", required.len());
    println!("finished: {}", computed);
    println!(
        "progress: {:.2}%",
        (computed as f64) / (required.len() as f64) * 100.0
    );

    Ok(())
}
