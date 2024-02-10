use std::error::Error;

use clap::Parser;
use simple_logger::SimpleLogger;

use flights::BlobStorageProvider;
use flights::{load_aircrafts, load_private_jet_models};

#[derive(clap::ValueEnum, Debug, Clone)]
enum Backend {
    Disk,
    Remote,
}

const ABOUT: &'static str = r#"Exports the database of all worldwide aircrafts whose primary use is to be a private jet to "data.csv"
and its description at `description.md` (in disk).
If `access_key` and `secret_access_key` is provided, data is written to the public blob storage instead.
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

    // load datasets to memory
    let aircrafts = load_aircrafts(client.as_ref()).await?;
    let models = load_private_jet_models()?;

    let private_jets = aircrafts
        .values()
        // its primary use is to be a private jet
        .filter(|a| models.contains_key(&a.model))
        .collect::<Vec<_>>();

    let mut wtr = csv::Writer::from_writer(vec![]);
    for jet in private_jets {
        wtr.serialize(jet).unwrap()
    }
    let data_csv = wtr.into_inner().unwrap();
    let specification_md = r#"This dataset was created according to
[this methodology](https://github.com/jorgecardleitao/private-jets/blob/main/methodology.md).

It contains 3 columns:
* `icao_number`: The transponder identifier
* `tail_number`: The tail number of the aircraft
* `model`: The icao number of the aircraft type. It is only one of the ones
  identified as private jet according to the methodology.
* `country`: The country (ISO 3166) of registration

Both `icao_number` and `tail_number` are unique keys (independently).
"#;

    if client.as_ref().map(|c| c.can_put()).unwrap_or(false) {
        let client = client.unwrap();
        client
            .put("database/private_jets/2023/11/06/data.csv", data_csv)
            .await?;
        client
            .put(
                "database/private_jets/2023/11/06/description.md",
                specification_md.as_bytes().to_vec(),
            )
            .await?;
    } else {
        std::fs::write("data.csv", data_csv)?;
        std::fs::write("description.md", specification_md.as_bytes())?;
    }
    Ok(())
}
