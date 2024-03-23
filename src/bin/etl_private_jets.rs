use std::error::Error;

use clap::Parser;
use flights::LocalDisk;
use simple_logger::SimpleLogger;

use flights::aircraft;
use flights::load_private_jet_models;
use flights::BlobStorageProvider;

#[derive(clap::ValueEnum, Debug, Clone)]
enum Backend {
    Disk,
    Remote,
}

const ABOUT: &'static str = r#"Exports the database of all worldwide aircrafts whose primary use is to be a private jet to "data.csv"
and its description at `description.md` (in disk).
If `access_key` and `secret_access_key` is provided, data is written to the public blob storage instead.
"#;

const SPECIFICATION: &'static str = r#"This dataset was created according to
[this methodology](https://github.com/jorgecardleitao/private-jets/blob/main/methodology.md).

It contains 3 columns:
* `icao_number`: The transponder identifier
* `tail_number`: The tail number of the aircraft
* `model`: The icao number of the aircraft type. It is only one of the ones
  identified as private jet according to the methodology.
* `country`: The country (ISO 3166) of registration

Both `icao_number` and `tail_number` are unique keys (independently).
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
    #[arg(short, long, value_enum, default_value_t=Backend::Remote)]
    backend: Backend,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let cli = Cli::parse();

    // initialize client
    let client = match (cli.backend, cli.access_key, cli.secret_access_key) {
        (Backend::Disk, _, _) => None,
        (_, Some(access_key), Some(secret_access_key)) => {
            Some(flights::fs_s3::client(access_key, secret_access_key).await)
        }
        (Backend::Remote, _, _) => Some(flights::fs_s3::anonymous_client().await),
    };
    let client = client
        .as_ref()
        .map(|x| x as &dyn BlobStorageProvider)
        .unwrap_or(&LocalDisk);

    // create db of all aircrafts as of now
    aircraft::etl_aircrafts(client).await?;

    // load datasets to memory
    let date = time::OffsetDateTime::now_utc().date();
    let aircrafts = aircraft::read(date, client).await?;
    let models = load_private_jet_models()?;

    let private_jets = aircrafts
        .values()
        // its primary use is to be a private jet
        .filter(|a| models.contains_key(&a.model));

    let data_csv = flights::csv::serialize(private_jets);

    if client.can_put() {
        client.put("private_jets/all.csv", data_csv).await?;
        client
            .put(
                "private_jets/description.md",
                SPECIFICATION.as_bytes().to_vec(),
            )
            .await?;
    } else {
        std::fs::write("data.csv", data_csv)?;
        std::fs::write("description.md", SPECIFICATION.as_bytes())?;
    }
    Ok(())
}
