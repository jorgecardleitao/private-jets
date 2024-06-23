use std::error::Error;

use clap::Parser;
use simple_logger::SimpleLogger;
use time::macros::date;
use tinytemplate::TinyTemplate;

use crate::aircraft::Aircrafts;
use flights::*;

static TEMPLATE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/bin/single_day_template.md"
));
static TEMPLATE_NAME: &'static str = "t";

#[derive(::serde::Serialize, ::serde::Deserialize, Debug)]
pub struct Event {
    pub tail_number: String,
    pub owner: Fact<Company>,
    pub date: String,
    pub from_airport: String,
    pub to_airport: String,
    pub two_way: bool,
    pub commercial_emissions_kg: Fact<usize>,
    pub emissions_kg: Fact<usize>,
    pub source: String,
    pub source_date: String,
}

#[derive(::serde::Serialize)]
pub struct Context {
    pub event: Event,
    pub dane_emissions_kg: Fact<usize>,
    pub dane_years: String,
}

#[derive(clap::ValueEnum, Debug, Clone)]
enum Backend {
    Disk,
    Remote,
}

const ABOUT: &'static str = r#"Writes a markdown file per leg (named `{tail-number}_{date}_{leg}.md`) on disk with a description of:
* the owner of said tail number
* the from and to
* how many emissions (CO2e) were emitted
* how many emissions (CO2e) would have been emitted if a commercial flight would
  have been taken instead.
* how many emissions per year (CO2e/y) a Dane emits
* The source of each of the claims
"#;

#[derive(Parser, Debug)]
#[command(author, version, about = ABOUT)]
struct Cli {
    /// The tail number
    #[arg(short, long)]
    tail_number: String,
    /// The date in format `yyyy-mm-dd`
    #[arg(short, long, value_parser = parse_date)]
    date: time::Date,
    /// The token to the remote storage
    #[arg(long)]
    access_key: Option<String>,
    /// The token to the remote storage
    #[arg(long)]
    secret_access_key: Option<String>,
    /// The backend to read cached data from.
    #[arg(short, long, value_enum, default_value_t=Backend::Remote)]
    backend: Backend,
}

fn parse_date(arg: &str) -> Result<time::Date, time::error::Parse> {
    time::Date::parse(
        arg,
        time::macros::format_description!("[year]-[month]-[day]"),
    )
}

async fn flight_date(
    tail_number: &str,
    date: time::Date,
    owners: &Owners,
    aircraft_owners: &AircraftOwners,
    aircrafts: &Aircrafts,
    client: &dyn BlobStorageProvider,
) -> Result<Vec<Event>, Box<dyn Error>> {
    let aircraft = aircrafts
        .get(tail_number)
        .ok_or_else(|| Into::<Box<dyn Error>>::into("Aircraft transponder number"))?;
    let icao = &aircraft.icao_number;
    log::info!("transponder number: {}", icao);

    let positions = positions(icao, date, client).await?;
    let legs = legs(positions);

    log::info!("Number of legs: {}", legs.len());

    let models = load_private_jet_models()?;
    let airports = airports_cached().await?;
    let aircraft_owner = aircraft_owners
        .get(tail_number)
        .ok_or_else(|| Into::<Box<dyn Error>>::into("Owner of tail number not found"))?;
    log::info!("Aircraft owner: {}", aircraft_owner.owner);
    let company = owners
        .get(&aircraft_owner.owner)
        .ok_or_else(|| Into::<Box<dyn Error>>::into("Owner not found"))?;
    log::info!("Owner information found");
    let owner = Fact {
        claim: company.clone(),
        source: aircraft_owner.source.clone(),
        date: aircraft_owner.date.clone(),
    };

    let consumption = models
        .get(&aircraft.model)
        .ok_or_else(|| Into::<Box<dyn Error>>::into("Consumption not found"))?;
    log::info!("Consumption: {} [gallon/h]", consumption.gph);

    Ok(legs.into_iter().map(|leg| {
        let commercial_emissions_kg = Fact {
            claim: emissions(leg.from().pos(), leg.to().pos(), Class::First) as usize,
            source: "https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/".to_string(),
            date: "2023-10-19".to_string()
        };
        let emissions_kg = Fact {
            claim: leg_per_person(leg_co2e_kg(consumption.gph as f64, leg.duration())) as usize,
            source: "See [methodology M-7](https://github.com/jorgecardleitao/private-jets/blob/main/methodology.md)".to_string(),
            date: time::OffsetDateTime::now_utc().date().to_string(),
        };

        Event {
            tail_number: tail_number.to_string(),
            owner: owner.clone(),
            date: date.to_string(),
            from_airport: closest(leg.from().pos(), &airports).name.clone(),
            to_airport: closest(leg.to().pos(), &airports).name.clone(),
            two_way: false,
            commercial_emissions_kg,
            emissions_kg,
            source: format!("https://globe.adsbexchange.com/?icao={icao}&showTrace={date}"),
            source_date: date.to_string(),
        }
    }).collect())
}

fn process_leg(
    event: Event,
    dane_emissions_kg: Fact<usize>,
    leg: usize,
) -> Result<(), Box<dyn Error>> {
    let path = format!("{}_{}_{leg}.md", event.tail_number, event.date);

    let dane_years = format!(
        "{:.2}",
        event.emissions_kg.claim as f32 / dane_emissions_kg.claim as f32
    );

    let context = Context {
        event,
        dane_emissions_kg,
        dane_years,
    };

    let mut tt = TinyTemplate::new();
    tt.set_default_formatter(&tinytemplate::format_unescaped);
    tt.add_template(TEMPLATE_NAME, TEMPLATE)?;

    let rendered = tt.render(TEMPLATE_NAME, &context)?;

    log::info!("Story written to {path}");
    std::fs::write(path, rendered)?;

    Ok(())
}

#[tokio::main]
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

    let owners = load_owners()?;
    let aircraft_owners = load_aircraft_owners()?;
    let aircrafts = aircraft::read(date!(2023 - 11 - 06), client).await?;

    let dane_emissions_kg = Fact {
        claim: 5100,
        source: "https://ourworldindata.org/co2/country/denmark Denmark emits 5.1 t CO2/person/year in 2019.".to_string(),
        date: "2023-10-08".to_string(),
    };

    let mut events = flight_date(
        &cli.tail_number,
        cli.date,
        &owners,
        &aircraft_owners,
        &aircrafts,
        client,
    )
    .await?;

    if events.len() == 2 && events[0].from_airport == events[1].to_airport {
        let mut event = events.remove(0);
        event.two_way = true;
        event.emissions_kg.claim *= 2;
        event.commercial_emissions_kg.claim *= 2;
        process_leg(event, dane_emissions_kg.clone(), 0)?;
    } else {
        for (leg, event) in events.into_iter().enumerate() {
            process_leg(event, dane_emissions_kg.clone(), leg)?;
        }
    }

    Ok(())
}
