use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::Arc,
};

use clap::Parser;
use flights::{Aircraft, AircraftModels, BlobStorageProvider, Leg};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use serde::Serialize;
use simple_logger::SimpleLogger;

static DATABASE_ROOT: &'static str = "leg/v1/";
static DATABASE: &'static str = "leg/v1/data/";

#[derive(serde::Serialize, serde::Deserialize)]
struct LegOut {
    tail_number: String,
    model: String,
    start: String,
    end: String,
    from_lat: f64,
    from_lon: f64,
    to_lat: f64,
    to_lon: f64,
    distance: f64,
    duration: f64,
    commercial_emissions_kg: usize,
    emissions_kg: usize,
}

#[derive(serde::Serialize)]
struct Metadata {
    icao_months_to_process: usize,
    icao_months_processed: usize,
}

async fn write_json(
    client: &impl BlobStorageProvider,
    d: impl Serialize,
    key: &str,
) -> Result<(), Box<dyn Error>> {
    let mut bytes: Vec<u8> = Vec::new();
    serde_json::to_writer(&mut bytes, &d).map_err(std::io::Error::other)?;

    Ok(client
        .put(&format!("{DATABASE_ROOT}{key}.json"), bytes)
        .await?)
}

async fn write_csv<B: BlobStorageProvider>(
    items: impl Iterator<Item = impl Serialize>,
    key: &str,
    client: &B,
) -> Result<(), B::Error> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    for leg in items {
        wtr.serialize(leg).unwrap()
    }
    let data_csv = wtr.into_inner().unwrap();
    client.put(&key, data_csv).await?;
    Ok(())
}

async fn write(
    icao_number: &Arc<str>,
    month: time::Date,
    legs: Vec<Leg>,
    private_jets: &HashMap<Arc<str>, Aircraft>,
    models: &AircraftModels,
    client: &impl BlobStorageProvider,
) -> Result<(), Box<dyn Error>> {
    let legs = legs.into_iter().map(|leg| {
        let aircraft = private_jets.get(icao_number).expect(icao_number);
        LegOut {
            tail_number: aircraft.tail_number.to_string(),
            model: aircraft.model.to_string(),
            start: leg.from().datetime().to_string(),
            end: leg.to().datetime().to_string(),
            from_lat: leg.from().latitude(),
            from_lon: leg.from().longitude(),
            to_lat: leg.to().latitude(),
            to_lon: leg.to().longitude(),
            distance: leg.distance(),
            duration: leg.duration().as_seconds_f64() / 60.0 / 60.0,
            commercial_emissions_kg: flights::emissions(
                leg.from().pos(),
                leg.to().pos(),
                flights::Class::First,
            ) as usize,
            emissions_kg: flights::leg_co2e_kg(
                models.get(&aircraft.model).expect(&aircraft.model).gph as f64,
                leg.duration(),
            ) as usize,
        }
    });

    let key = format!(
        "{DATABASE}icao_number={icao_number}/month={}/data.csv",
        flights::month_to_part(&month)
    );

    write_csv(legs, &key, client).await?;
    log::info!("Written {} {}", icao_number, month);
    Ok(())
}

async fn read(
    icao_number: &Arc<str>,
    month: time::Date,
    client: &impl BlobStorageProvider,
) -> Result<Vec<LegOut>, Box<dyn Error>> {
    let key = format!(
        "{DATABASE}icao_number={icao_number}/month={}/data.csv",
        flights::month_to_part(&month)
    );
    let content = client.maybe_get(&key).await?.expect("File to be present");

    csv::Reader::from_reader(&content[..])
        .deserialize()
        .map(|x| {
            let record: LegOut = x?;
            Ok(record)
        })
        .collect()
}

async fn private_jets(
    client: Option<&flights::fs_s3::ContainerClient>,
    country: Option<&str>,
) -> Result<Vec<Aircraft>, Box<dyn std::error::Error>> {
    // load datasets to memory
    let aircrafts = flights::load_aircrafts(client).await?;
    let models = flights::load_private_jet_models()?;

    Ok(aircrafts
        .into_iter()
        // its primary use is to be a private jet
        .filter(|(_, a)| {
            country
                .map(|country| a.country.as_deref() == Some(country))
                .unwrap_or(true)
        })
        .filter_map(|(_, a)| models.contains_key(&a.model).then_some(a))
        .collect())
}

const ABOUT: &'static str = r#"Builds the database of all legs"#;

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

    let models = flights::load_private_jet_models()?;

    let months = (2023..2024).cartesian_product(1..=12u8).count();
    let relevant_jets = private_jets(Some(&client), cli.country.as_deref())
        .await?
        .into_iter()
        .map(|a| (a.icao_number.clone(), a))
        .collect::<HashMap<_, _>>();
    let required = relevant_jets.len() * months;
    log::info!("required : {}", required);

    let ready = flights::existing_months_positions(&client)
        .await?
        .into_iter()
        .filter(|(icao, _)| relevant_jets.contains_key(icao))
        .collect::<HashSet<_>>();
    log::info!("ready    : {}", ready.len());

    let completed = flights::existing(DATABASE, &client)
        .await?
        .into_iter()
        .filter(|(icao, _)| relevant_jets.contains_key(icao))
        .collect::<HashSet<_>>();
    log::info!("completed: {}", completed.len());

    let mut todo = ready.difference(&completed).collect::<Vec<_>>();
    todo.sort_unstable_by_key(|(icao, date)| (date, icao));
    log::info!("todo     : {}", todo.len());

    let client = Some(&client);
    let relevant_jets = &relevant_jets;
    let models = &models;

    let tasks = todo.into_iter().map(|(icao_number, month)| async move {
        let positions = flights::month_positions(*month, &icao_number, client).await?;
        let legs = flights::legs(positions.into_iter());
        write(
            &icao_number,
            *month,
            legs,
            &relevant_jets,
            &models,
            client.unwrap(),
        )
        .await
    });

    let _ = futures::stream::iter(tasks)
        .buffered(20)
        .try_collect::<Vec<_>>()
        .await?;

    let private_jets = private_jets(client, None)
        .await?
        .into_iter()
        .map(|a| (a.icao_number.clone(), a))
        .collect::<HashMap<_, _>>();

    let client = client.unwrap();
    let completed = flights::existing(DATABASE, client)
        .await?
        .into_iter()
        .filter(|(icao, _)| private_jets.contains_key(icao))
        .collect::<HashSet<_>>();

    let tasks = completed
        .iter()
        .map(|(icao, date)| async move { read(icao, *date, client).await });

    log::info!("Gettings all legs");
    let legs = futures::stream::iter(tasks)
        .buffered(20)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten();

    let key = format!("{DATABASE_ROOT}all.csv");
    write_csv(legs, &key, client).await?;
    log::info!("Written {key}");

    write_json(
        client,
        Metadata {
            icao_months_to_process: required,
            icao_months_processed: completed.len(),
        },
        "status",
    )
    .await?;
    log::info!("status written");

    Ok(())
}
