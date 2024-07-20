use std::error::Error;

use clap::Parser;
use futures::StreamExt;
use simple_logger::SimpleLogger;

const ABOUT: &'static str = r#"Builds the database of all private jet positions since 2019"#;

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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let cli = Cli::parse();

    let client = flights::fs_s3::client(cli.access_key, cli.secret_access_key).await;

    let required =
        flights::private_jets_in_month((2019..2025).rev(), cli.country.as_deref(), &client).await?;

    log::info!("required : {}", required.len());

    let completed = flights::icao_to_trace::list_months_positions(&client).await?;
    log::info!("completed: {}", completed.len());
    let mut todo = required.difference(&completed).collect::<Vec<_>>();
    todo.sort_unstable_by_key(|(icao_number, date)| (date, icao_number));
    log::info!("todo     : {}", todo.len());

    let tasks = todo.into_iter().map(|(icao_number, month)| {
        flights::icao_to_trace::month_positions(icao_number, *month, &client)
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
