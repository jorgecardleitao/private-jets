use std::{collections::HashSet, error::Error};

use clap::Parser;
use futures::StreamExt;
use itertools::Itertools;
use simple_logger::SimpleLogger;
use time::macros::date;

use flights::{aircraft, existing_months_positions, BlobStorageProvider};

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
    /// Optional country to fetch from (in ISO 3166); defaults to whole world
    #[arg(long)]
    country: Option<String>,
}

async fn private_jets(
    client: &dyn BlobStorageProvider,
    country: Option<&str>,
) -> Result<Vec<aircraft::Aircraft>, Box<dyn std::error::Error>> {
    // load datasets to memory
    let aircrafts = aircraft::read(date!(2023 - 11 - 06), client).await?;
    let models = flights::load_private_jet_models()?;

    Ok(aircrafts
        .into_iter()
        // its primary use is to be a private jet
        .filter_map(|(_, a)| models.contains_key(&a.model).then_some(a))
        .filter(|a| {
            country
                .map(|country| a.country.as_deref() == Some(country))
                .unwrap_or(true)
        })
        .collect())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let cli = Cli::parse();

    let client = flights::fs_s3::client(cli.access_key, cli.secret_access_key).await;

    let months = (2019..2024)
        .rev()
        .cartesian_product(1..=12u8)
        .map(|(year, month)| {
            time::Date::from_calendar_date(year, time::Month::try_from(month).unwrap(), 1)
                .expect("day 1 never errors")
        });
    let private_jets = private_jets(&client, cli.country.as_deref()).await?;
    log::info!("jets     : {}", private_jets.len());
    let required = private_jets
        .into_iter()
        .map(|a| a.icao_number)
        .cartesian_product(months)
        .collect::<HashSet<_>>();
    log::info!("required : {}", required.len());

    let completed = existing_months_positions(&client).await?;
    log::info!("completed: {}", completed.len());
    let mut todo = required.difference(&completed).collect::<Vec<_>>();
    todo.sort_unstable_by_key(|(icao, date)| (date, icao));
    log::info!("todo     : {}", todo.len());

    let tasks = todo
        .into_iter()
        .map(|(icao_number, month)| flights::month_positions(*month, icao_number, &client));

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
