use std::error::Error;

use clap::Parser;
use futures::StreamExt;
use itertools::Itertools;
use simple_logger::SimpleLogger;

use flights::{existing_months_positions, load_aircrafts, load_private_jet_models};

const ABOUT: &'static str = r#"Builds the database of all private jet positions from 2023"#;

#[derive(Parser, Debug)]
#[command(author, version, about = ABOUT)]
struct Cli {
    /// The Azure token
    #[arg(short, long)]
    azure_sas_token: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Warn)
        .init()
        .unwrap();

    let cli = Cli::parse();

    // optionally initialize Azure client
    let client = match cli.azure_sas_token.clone() {
        None => flights::fs_azure::initialize_anonymous("privatejets", "data"),
        Some(token) => flights::fs_azure::initialize_sas(&token, "privatejets", "data")?,
    };

    // load datasets to memory
    let aircrafts = load_aircrafts(Some(&client)).await?;
    let models = load_private_jet_models()?;

    let private_jets = aircrafts
        .values()
        // its primary use is to be a private jet
        .filter(|a| models.contains_key(&a.model))
        .collect::<Vec<_>>();

    let months = (2023..2024)
        .cartesian_product(1..=12u8)
        .map(|(year, month)| {
            time::Date::from_calendar_date(year, time::Month::try_from(month).unwrap(), 1)
                .expect("day 1 never errors")
        })
        .collect::<Vec<_>>();

    let completed = existing_months_positions(&months, &client, 50).await?;

    let required = private_jets
        .into_iter()
        .cartesian_product(months.into_iter())
        .filter(|(a, date)| !completed.contains(&(a.icao_number.clone(), *date)));

    let tasks = required.map(|(aircraft, month)| {
        flights::month_positions(month, &aircraft.icao_number, Some(&client))
    });

    futures::stream::iter(tasks)
        // limit concurrent tasks
        .buffered(10)
        // continue if error
        .map(|r| {
            if let Err(e) = r {
                log::error!("{e}");
            }
        })
        .collect::<Vec<_>>()
        .await;
    Ok(())
}
