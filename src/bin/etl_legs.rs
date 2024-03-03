use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::Arc,
};

use clap::Parser;
use flights::{Aircraft, AircraftModels, BlobStorageProvider, Leg};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};
use simple_logger::SimpleLogger;

static DATABASE_ROOT: &'static str = "leg/v1/";
static DATABASE: &'static str = "leg/v1/data/";

#[derive(serde::Serialize, serde::Deserialize)]
struct LegOut {
    tail_number: String,
    model: String,
    #[serde(with = "time::serde::rfc3339")]
    start: time::OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    end: time::OffsetDateTime,
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
    url: String,
}

async fn write_json(
    client: &impl BlobStorageProvider,
    d: impl Serialize,
    key: &str,
) -> Result<(), Box<dyn Error>> {
    let mut bytes: Vec<u8> = Vec::new();
    serde_json::to_writer(&mut bytes, &d).map_err(std::io::Error::other)?;

    Ok(client.put(key, bytes).await?)
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

fn transform<'a>(
    icao_number: &'a Arc<str>,
    legs: Vec<Leg>,
    private_jets: &'a HashMap<Arc<str>, Aircraft>,
    models: &'a AircraftModels,
) -> impl Iterator<Item = LegOut> + 'a {
    legs.into_iter().map(|leg| {
        let aircraft = private_jets.get(icao_number).expect(icao_number);
        LegOut {
            tail_number: aircraft.tail_number.to_string(),
            model: aircraft.model.to_string(),
            start: leg.from().datetime(),
            end: leg.to().datetime(),
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
    })
}

async fn write(
    icao_number: &Arc<str>,
    month: time::Date,
    legs: impl Iterator<Item = impl Serialize>,
    client: &impl BlobStorageProvider,
) -> Result<(), Box<dyn Error>> {
    let key = format!(
        "{DATABASE}icao_number={icao_number}/month={}/data.csv",
        flights::month_to_part(&month)
    );

    write_csv(legs, &key, client).await?;
    log::info!("Written {} {}", icao_number, month);
    Ok(())
}

async fn read<D: DeserializeOwned>(
    icao_number: &Arc<str>,
    month: time::Date,
    client: &impl BlobStorageProvider,
) -> Result<Vec<D>, Box<dyn Error>> {
    let key = format!(
        "{DATABASE}icao_number={icao_number}/month={}/data.csv",
        flights::month_to_part(&month)
    );
    let content = client.maybe_get(&key).await?.expect("File to be present");

    csv::Reader::from_reader(&content[..])
        .deserialize::<D>()
        .map(|x| Ok(x?))
        .collect()
}

async fn private_jets(
    client: Option<&flights::fs_s3::ContainerClient>,
) -> Result<Vec<Aircraft>, Box<dyn std::error::Error>> {
    // load datasets to memory
    let aircrafts = flights::load_aircrafts(client).await?;
    let models = flights::load_private_jet_models()?;

    Ok(aircrafts
        .into_iter()
        // its primary use is to be a private jet
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

async fn etl_task(
    icao_number: &Arc<str>,
    month: time::Date,
    private_jets: &HashMap<Arc<str>, Aircraft>,
    models: &AircraftModels,
    client: Option<&flights::fs_s3::ContainerClient>,
) -> Result<(), Box<dyn Error>> {
    // extract
    let positions = flights::month_positions(month, &icao_number, client).await?;
    // transform
    let legs = transform(
        &icao_number,
        flights::legs(positions.into_iter()),
        &private_jets,
        &models,
    );
    // load
    write(&icao_number, month, legs, client.unwrap()).await
}

async fn aggregate(
    private_jets: Vec<Aircraft>,
    client: &flights::fs_s3::ContainerClient,
) -> Result<(), Box<dyn Error>> {
    let models = flights::load_private_jet_models()?;

    let private_jets = private_jets
        .into_iter()
        .map(|a| (a.icao_number.clone(), a))
        .collect::<HashMap<_, _>>();

    let completed = flights::existing(DATABASE, client)
        .await?
        .into_iter()
        .filter(|(icao, _)| private_jets.contains_key(icao))
        .collect::<HashSet<_>>();

    // group completed by year
    let by_year = completed
        .into_iter()
        .fold(HashMap::<i32, HashSet<_>>::new(), |mut acc, v| {
            acc.entry(v.1.year())
                .and_modify(|entries| {
                    entries.insert(v.clone());
                })
                .or_insert(HashSet::from([v]));
            acc
        });

    // run tasks by year
    let private_jets = &private_jets;
    let models = &models;
    let mut metadata = HashMap::<i32, Metadata>::new();
    for (year, completed) in by_year {
        let tasks = completed.iter().map(|(icao_number, date)| async move {
            let r = read::<LegOut>(icao_number, *date, client).await;
            if let Err(_) = r {
                etl_task(icao_number, *date, private_jets, models, Some(client)).await?;
                return read::<LegOut>(icao_number, *date, client).await;
            } else {
                r
            }
        });

        log::info!("Gettings all legs for year={year}");
        let legs = futures::stream::iter(tasks)
            .buffered(100)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten();

        log::info!("Writing all legs for year={year}");
        let key = format!("{DATABASE_ROOT}all/year={year}/data.csv");
        write_csv(legs, &key, client).await?;
        log::info!("Written {key}");
        metadata.insert(
            year,
            Metadata {
                icao_months_to_process: private_jets.len() * 12,
                icao_months_processed: completed.len(),
                url: format!("https://fra1.digitaloceanspaces.com/{key}"),
            },
        );
    }

    let key = format!("{DATABASE_ROOT}status.json");
    write_json(client, metadata, &key).await?;
    log::info!("status written");
    Ok(())
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

    let months = (2020..2024).cartesian_product(1..=12u8).count();
    let private_jets = private_jets(Some(&client)).await?;
    let relevant_jets = private_jets
        .clone()
        .into_iter()
        // in the country
        .filter(|a| {
            cli.country
                .as_deref()
                .map(|country| a.country.as_deref() == Some(country))
                .unwrap_or(true)
        })
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
        etl_task(icao_number, *month, &relevant_jets, &models, client).await
    });

    let _ = futures::stream::iter(tasks)
        .buffered(20)
        .try_collect::<Vec<_>>()
        .await?;

    aggregate(private_jets, client.unwrap()).await
}
