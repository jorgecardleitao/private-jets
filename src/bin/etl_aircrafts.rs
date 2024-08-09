use std::error::Error;

use clap::Parser;
use simple_logger::SimpleLogger;

use flights::aircraft;
use flights::fs;

#[derive(clap::ValueEnum, Debug, Clone)]
enum Backend {
    Disk,
    Remote,
}

const ABOUT: &'static str = r#"Creates a new snapshot of the database of all worldwide aircrafts according to `M-aircrafts-in-time`.
This ETL is append only - every time it runs, it creates a new snapshot.
If `access_key` and `secret_access_key` are not provided, data is written to the local disk.
"#;

#[derive(Parser, Debug)]
#[command(author, version, about = ABOUT)]
struct Cli {
    /// The token to the remote storage
    #[arg(long)]
    access_key: Option<String>,
    /// The token to the remote storage
    #[arg(long)]
    secret_access_key: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let cli = Cli::parse();

    // initialize client
    let client = match (cli.access_key, cli.secret_access_key) {
        (Some(access_key), Some(secret_access_key)) => {
            Some(flights::fs_s3::client(access_key, secret_access_key).await)
        }
        (None, None) => None,
        _ => {
            return Err("both access_key and secret_access_key must be provided or neither".into())
        }
    };
    let client = client
        .as_ref()
        .map(|x| x as &dyn fs::BlobStorageProvider)
        .unwrap_or(&fs::LocalDisk);

    aircraft::etl_aircrafts(client).await?;

    // write private jets to dedicated place.
    let data = std::fs::read_to_string("src/models.csv")?;
    client
        .put("model/db/data.csv", data.as_bytes().to_vec())
        .await?;

    Ok(())
}
