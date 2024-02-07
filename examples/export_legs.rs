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
    /// The token to the remote storage
    #[arg(long)]
    access_key: String,
    /// The token to the remote storage
    #[arg(long)]
    secret_access_key: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Warn)
        .init()
        .unwrap();

    let cli = Cli::parse();

    let client = flights::fs_s3::client(cli.access_key, cli.secret_access_key).await;

    // load datasets to memory
    let aircrafts = load_aircrafts(Some(&client)).await?;
    let models = load_private_jet_models()?;

    let completed = existing_months_positions(&client).await?;
    log::info!("already computed: {}", completed.len());

    let private_jets = aircrafts
        .values()
        // its primary use is to be a private jet
        .filter(|a| models.contains_key(&a.model));

    let months = (2023..2024)
        .cartesian_product(1..=12u8)
        .map(|(year, month)| {
            time::Date::from_calendar_date(year, time::Month::try_from(month).unwrap(), 1)
                .expect("day 1 never errors")
        });

    let required = private_jets
        .cartesian_product(months)
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
