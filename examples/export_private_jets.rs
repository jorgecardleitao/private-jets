use std::error::Error;

use clap::Parser;
use simple_logger::SimpleLogger;

use flights::BlobStorageProvider;
use flights::{load_aircrafts, load_private_jet_models};

#[derive(clap::ValueEnum, Debug, Clone)]
enum Backend {
    Disk,
    Azure,
}

const ABOUT: &'static str = r#"Exports the database of all worldwide aircrafts whose primary use is to be a private jet to "data.csv"
and its description at `description.md` (in disk).
If `azure_sas_token` is provided, data is written to the public blob storage instead.
"#;

#[derive(Parser, Debug)]
#[command(author, version, about = ABOUT)]
struct Cli {
    /// The Azure token
    #[arg(short, long)]
    azure_sas_token: Option<String>,
    #[arg(short, long, value_enum, default_value_t=Backend::Azure)]
    backend: Backend,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
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

Both `icao_number` and `tail_number` are unique keys (independently).
"#;

    if cli.azure_sas_token.is_some() {
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
