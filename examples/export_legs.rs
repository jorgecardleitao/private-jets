use std::error::Error;

use clap::Parser;
use futures::StreamExt;
use itertools::Itertools;
use simple_logger::SimpleLogger;

use flights::{
    existing_months_positions, load_aircrafts, load_private_jet_models, month_positions,
};

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
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let cli = Cli::parse();
    let client = flights::fs_s3::client(cli.access_key, cli.secret_access_key).await;

    // load datasets to memory
    //let aircrafts = load_aircrafts(Some(&client)).await?;
    //let models = load_private_jet_models()?;

    let completed = existing_months_positions(&client).await?;
    log::info!("already computed: {}", completed.len());

    let a = Some(&client);
    let tasks = completed
        .into_iter()
        .map(|(icao, date)| async move { month_positions(date.clone(), &icao, a).await });

    futures::stream::iter(tasks)
        // limit concurrent tasks
        .buffered(100)
        // continue if error
        .map(|r| {
            if let Err(e) = r {
                log::error!("{e}");
            }
        })
        .collect::<Vec<_>>()
        .await;
    return Ok(());
}
