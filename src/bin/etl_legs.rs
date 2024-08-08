use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::Arc,
};

use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};
use simple_logger::SimpleLogger;

use flights::{
    aircraft::{Aircraft, Aircrafts},
    fs::BlobStorageProvider,
    Position,
};

static DATABASE_ROOT: &'static str = "leg/v2/";
static DATABASE: &'static str = "leg/v2/data/";

#[derive(serde::Serialize, serde::Deserialize)]
struct LegOut {
    /// The ICAO number
    icao_number: Arc<str>,
    /// The tail number
    tail_number: Arc<str>,
    /// The aircraft model
    aircraft_model: Arc<str>,
    /// The start timestamp
    #[serde(with = "time::serde::rfc3339")]
    start: time::OffsetDateTime,
    /// The start latitude
    start_lat: f64,
    /// The start longitude
    start_lon: f64,
    /// The start altitude in feet
    start_altitude: f64,
    /// The end timestamp
    #[serde(with = "time::serde::rfc3339")]
    end: time::OffsetDateTime,
    /// The end latitude
    end_lat: f64,
    /// The end longitude
    end_lon: f64,
    /// The end altitude in feet
    end_altitude: f64,
    /// The total two-dimensional length of the leg in km
    length: f64,
    /// The great-circle distance of the leg in km
    great_circle_distance: f64,
    /// The time above 30.000 feet
    hours_above_30000: f64,
    /// The time above 40.000 feet
    hours_above_40000: f64,
}

#[derive(serde::Serialize)]
struct Metadata {
    icao_months_to_process: usize,
    icao_months_processed: usize,
    url: String,
}

async fn write_json(
    client: &dyn BlobStorageProvider,
    d: impl Serialize,
    key: &str,
) -> Result<(), Box<dyn Error>> {
    let mut bytes: Vec<u8> = Vec::new();
    serde_json::to_writer(&mut bytes, &d).map_err(std::io::Error::other)?;

    Ok(client.put(key, bytes).await?)
}

async fn write_csv(
    items: impl Iterator<Item = impl Serialize>,
    key: &str,
    client: &dyn BlobStorageProvider,
) -> Result<(), std::io::Error> {
    let data_csv = flights::csv::serialize(items);
    client.put(&key, data_csv).await?;
    Ok(())
}

fn transform<'a>(
    icao_number: &'a Arc<str>,
    aircraft: &'a Aircraft,
    positions: Vec<Position>,
) -> impl Iterator<Item = LegOut> + 'a {
    flights::legs::legs(positions.into_iter()).map(|leg| LegOut {
        icao_number: icao_number.clone(),
        tail_number: aircraft.tail_number.clone().into(),
        aircraft_model: aircraft.model.clone().into(),
        start: leg.from().datetime(),
        start_lat: leg.from().latitude(),
        start_lon: leg.from().longitude(),
        start_altitude: leg.from().altitude(),
        end: leg.to().datetime(),
        end_lat: leg.to().latitude(),
        end_lon: leg.to().longitude(),
        end_altitude: leg.to().altitude(),
        length: leg.length(),
        great_circle_distance: leg.great_circle_distance(),
        hours_above_30000: leg
            .positions()
            .windows(2)
            .filter_map(|w| {
                (w[0].altitude() > 30000.0 && w[1].altitude() > 30000.0).then(|| {
                    (w[1].datetime() - w[0].datetime()).whole_seconds() as f64 / 60.0 / 60.0
                })
            })
            .sum::<f64>(),
        hours_above_40000: leg
            .positions()
            .windows(2)
            .filter_map(|w| {
                (w[0].altitude() > 40000.0 && w[1].altitude() > 40000.0).then(|| {
                    (w[1].datetime() - w[0].datetime()).whole_seconds() as f64 / 60.0 / 60.0
                })
            })
            .sum::<f64>(),
    })
}

async fn write(
    icao: &Arc<str>,
    month: time::Date,
    legs: impl Iterator<Item = impl Serialize>,
    client: &dyn BlobStorageProvider,
) -> Result<(), Box<dyn Error>> {
    let key = pk_to_blob_name(icao, month);

    write_csv(legs, &key, client).await?;
    log::info!("Written {} {}", icao, month);
    Ok(())
}

async fn read<D: DeserializeOwned>(
    icao: &Arc<str>,
    month: time::Date,
    client: &dyn BlobStorageProvider,
) -> Result<Vec<D>, std::io::Error> {
    flights::io::get_csv(&pk_to_blob_name(icao, month), client).await
}

fn pk_to_blob_name(icao: &str, month: time::Date) -> String {
    let month = flights::serde::month_to_part(month);
    format!("{DATABASE}month={month}/icao_number={icao}/data.csv")
}

fn blob_name_to_pk(blob: &str) -> (Arc<str>, time::Date) {
    let keys = flights::serde::hive_to_map(&blob[DATABASE.len()..blob.len() - "data.csv".len()]);
    let icao = *keys.get("icao_number").unwrap();
    let date = *keys.get("month").unwrap();
    (icao.into(), flights::serde::parse_month(date))
}

/// Returns the set of (icao number, month) that exist in the container prefixed by `prefix`
async fn list(
    client: &dyn BlobStorageProvider,
) -> Result<HashSet<(Arc<str>, time::Date)>, std::io::Error> {
    Ok(client
        .list(DATABASE)
        .await?
        .into_iter()
        .map(|blob| blob_name_to_pk(&blob))
        .collect())
}

const ABOUT: &'static str = "Builds the database of all legs";

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
    aircraft: &Aircraft,
    month: time::Date,
    client: &dyn BlobStorageProvider,
) -> Result<(), Box<dyn Error>> {
    // extract
    let positions =
        flights::icao_to_trace::get_month_positions(&icao_number, month, client).await?;
    // transform
    let legs = transform(&icao_number, aircraft, positions);
    // load
    write(&icao_number, month, legs, client).await
}

async fn aggregate(
    required: HashMap<(Arc<str>, time::Date), Aircraft>,
    client: &dyn BlobStorageProvider,
) -> Result<(), Box<dyn Error>> {
    let all_completed = list(client).await?;

    let completed = all_completed
        .into_iter()
        .filter(|key| required.contains_key(key))
        .collect::<HashSet<_>>();

    // group completed by year
    let completed_by_year =
        completed
            .into_iter()
            .fold(HashMap::<i32, HashSet<_>>::new(), |mut acc, v| {
                acc.entry(v.1.year())
                    .and_modify(|entries| {
                        entries.insert(v.clone());
                    })
                    .or_insert(HashSet::from([v]));
                acc
            });
    let required_by_year =
        required
            .into_iter()
            .fold(HashMap::<i32, usize>::new(), |mut acc, (v, _)| {
                acc.entry(v.1.year())
                    .and_modify(|entries| {
                        *entries += 1;
                    })
                    .or_insert(0);
                acc
            });

    // run tasks by year
    let mut metadata = HashMap::<i32, Metadata>::new();
    for (year, completed) in completed_by_year {
        let tasks = completed.iter().map(|(icao_number, date)| async move {
            read::<LegOut>(icao_number, *date, client).await
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
                icao_months_to_process: *required_by_year.get(&year).unwrap(),
                icao_months_processed: completed.len(),
                url: format!("https://private-jets.fra1.digitaloceanspaces.com/{key}"),
            },
        );
    }

    let key = format!("{DATABASE_ROOT}status.json");
    write_json(client, metadata, &key).await?;
    log::info!("status written");
    Ok(())
}

async fn private_jets(
    client: &dyn BlobStorageProvider,
    country: Option<&str>,
) -> Result<Aircrafts, Box<dyn std::error::Error>> {
    // load datasets to memory
    let aircrafts = flights::aircraft::read(time::macros::date!(2023 - 11 - 06), client).await?;
    let models = flights::load_private_jet_models()?;

    Ok(aircrafts
        .into_iter()
        // its primary use is to be a private jet
        .filter(|(_, a)| models.contains_key(&a.model))
        .filter(|(_, a)| {
            country
                .map(|country| a.country.as_deref() == Some(country))
                .unwrap_or(true)
        })
        .collect())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let cli = Cli::parse();
    let maybe_country = cli.country.as_deref();

    let client = flights::fs_s3::client(cli.access_key, cli.secret_access_key).await;
    let client = &client;

    log::info!("computing required tasks...");
    let private_jets = private_jets(client, maybe_country).await?;

    let months = (2019..2024)
        .cartesian_product(1..=12u8)
        .map(|(year, month)| {
            time::Date::from_calendar_date(year, time::Month::try_from(month).unwrap(), 1)
                .expect("day 1 never errors")
        })
        .collect::<Vec<_>>();

    let required = private_jets
        .into_iter()
        .map(|(icao, aircraft)| {
            months
                .clone()
                .into_iter()
                .map(move |date| ((icao.clone(), date), aircraft.clone()))
        })
        .flatten()
        .collect::<HashMap<_, _>>();

    log::info!("required : {}", required.len());

    log::info!("computing completed tasks...");
    let completed = HashSet::new(); //list(client).await?.into_iter().collect::<HashSet<_>>();
    log::info!("completed: {}", completed.len());

    log::info!("computing ready tasks...");
    let ready = flights::icao_to_trace::list_months_positions(client)
        .await?
        .into_iter()
        .filter(|key| required.contains_key(key))
        .collect::<HashSet<_>>();
    log::info!("ready    : {}", ready.len());

    let mut todo = ready.difference(&completed).collect::<Vec<_>>();
    todo.sort_unstable_by_key(|(icao, date)| (date, icao));
    log::info!("todo     : {}", todo.len());

    log::info!("executing todos...");
    let tasks = todo.into_iter().map(|icao_month| async {
        let aircraft = required.get(icao_month).expect("limited to required above");
        let (icao_number, month) = icao_month;
        etl_task(icao_number, aircraft, *month, client).await
    });

    let _ = futures::stream::iter(tasks)
        .buffered(50)
        .collect::<Vec<_>>()
        .await;
    log::info!("todos completed");

    log::info!("aggregating...");
    aggregate(required, client).await
}
