use clap::Parser;

use flights::{fs_s3::ContainerClient, BlobStorageProvider};
use futures::StreamExt;
use simple_logger::SimpleLogger;

async fn delete(client: &ContainerClient) -> Result<(), Box<dyn std::error::Error>> {
    let tasks = client.list("position/icao_number=3b9b60").await?;

    log::info!("{}", tasks.len());
    let tasks = tasks
        .into_iter()
        .map(|blob| async move { client.delete(&blob).await });

    futures::stream::iter(tasks)
        // limit concurrent tasks
        .buffered(200)
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

#[derive(Parser, Debug)]
#[command(author, version)]
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let cli = Cli::parse();

    let client = flights::fs_s3::client(cli.access_key, cli.secret_access_key).await;

    delete(&client).await?;
    Ok(())
}
