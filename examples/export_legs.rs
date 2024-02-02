use std::error::Error;

use clap::Parser;
use futures::StreamExt;
use itertools::Itertools;
use simple_logger::SimpleLogger;

use flights::{load_aircrafts, load_private_jet_types};

#[derive(clap::ValueEnum, Debug, Clone)]
enum Backend {
    Disk,
    Azure,
}

const ABOUT: &'static str = r#"Builds the database of all private jet positions from 2023"#;

#[derive(Parser, Debug)]
#[command(author, version, about = ABOUT)]
struct Cli {
    /// The Azure token
    #[arg(short, long)]
    azure_sas_token: Option<String>,
    #[arg(short, long, value_enum, default_value_t=Backend::Azure)]
    backend: Backend,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Warn)
        .init()
        .unwrap();

    let cli = Cli::parse();

    // optionally initialize Azure client
    let client = match (cli.backend, cli.azure_sas_token.clone()) {
        (Backend::Disk, None) => None,
        (Backend::Azure, None) => Some(flights::fs_azure::initialize_anonymous(
            "privatejets",
            "data",
        )),
        (_, Some(token)) => Some(flights::fs_azure::initialize_sas(
            &token,
            "privatejets",
            "data",
        )?),
    };

    // load datasets to memory
    let aircrafts = load_aircrafts(client.as_ref()).await?;
    let types = load_private_jet_types()?;

    let private_jets = aircrafts
        .values()
        // its primary use is to be a private jet
        .filter(|a| types.contains_key(&a.type_designator))
        .collect::<Vec<_>>();

    let months = (2023..2024)
        .cartesian_product(1..=12u8)
        .map(|(year, month)| {
            time::Date::from_calendar_date(year, time::Month::try_from(month).unwrap(), 1)
                .expect("day 1 never errors")
        })
        .collect::<Vec<_>>();

    let required = private_jets
        .into_iter()
        .cartesian_product(months.into_iter())
        .collect::<Vec<_>>();

    let tasks = required.into_iter().map(|(aircraft, month)| {
        flights::month_positions(month, &aircraft.icao_number, client.as_ref())
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
