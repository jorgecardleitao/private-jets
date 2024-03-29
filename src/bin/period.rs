use std::error::Error;

use clap::Parser;
use simple_logger::SimpleLogger;
use time::macros::date;

use flights::{
    aircraft, aircraft::Aircraft, emissions, load_aircraft_owners, load_owners,
    BlobStorageProvider, Class, Company, Fact, LocalDisk,
};

#[derive(serde::Serialize)]
pub struct Context {
    pub owner: Fact<Company>,
    pub aircraft: Aircraft,
    pub from_date: String,
    pub to_date: String,
    pub number_of_legs: Fact<usize>,
    pub emissions_tons: Fact<usize>,
    pub dane_years: Fact<String>,
    pub number_of_legs_less_300km: usize,
    pub number_of_legs_more_300km: usize,
    pub ratio_commercial_300km: String,
}

static TEMPLATE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/bin/period_template.md"
));

fn render(context: &Context) -> Result<(), Box<dyn Error>> {
    let path = "story.md";

    let mut tt = tinytemplate::TinyTemplate::new();
    tt.set_default_formatter(&tinytemplate::format_unescaped);
    tt.add_template("t", TEMPLATE)?;

    let rendered = tt.render("t", context)?;

    log::info!("Story written to {path}");
    std::fs::write(path, rendered)?;
    Ok(())
}

#[derive(clap::ValueEnum, Debug, Clone)]
enum Backend {
    Disk,
    Remote,
}

fn parse_date(arg: &str) -> Result<time::Date, time::error::Parse> {
    time::Date::parse(
        arg,
        time::macros::format_description!("[year]-[month]-[day]"),
    )
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// The token to the remote storage
    #[arg(long)]
    access_key: Option<String>,
    /// The token to the remote storage
    #[arg(long)]
    secret_access_key: Option<String>,
    #[arg(long, value_enum, default_value_t=Backend::Remote)]
    backend: Backend,

    /// The tail number
    #[arg(long)]
    tail_number: String,
    /// A date in format `yyyy-mm-dd`
    #[arg(long, value_parser = parse_date)]
    from: time::Date,
    /// Optional end date in format `yyyy-mm-dd` (else it is to today)
    #[arg(long, value_parser = parse_date)]
    to: Option<time::Date>,
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

    // load datasets to memory
    let owners = load_owners()?;
    let aircraft_owners = load_aircraft_owners()?;
    let aircrafts = aircraft::read(date!(2023 - 11 - 06), client).await?;

    let from = cli.from;
    let to = cli.to.unwrap_or(time::OffsetDateTime::now_utc().date());

    let tail_number = &cli.tail_number;
    let aircraft = aircrafts
        .get(tail_number)
        .ok_or_else(|| Into::<Box<dyn Error>>::into("Aircraft ICAO number not found"))?
        .clone();
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

    let icao = &aircraft.icao_number;
    log::info!("ICAO number: {}", icao);

    let positions = flights::aircraft_positions(from, to, icao, client).await?;

    let legs = flights::legs(positions.into_iter());
    log::info!("number_of_legs: {}", legs.len());
    for leg in &legs {
        log::info!(
            "{},{},{},{},{},{},{},{}",
            leg.from().datetime(),
            leg.from().latitude(),
            leg.from().longitude(),
            leg.from().altitude(),
            leg.to().datetime(),
            leg.to().latitude(),
            leg.to().longitude(),
            leg.to().altitude(),
        );
    }

    let commercial_to_private_ratio = 10.0;
    let commercial_emissions_tons = legs
        .iter()
        .map(|leg| emissions(leg.from().pos(), leg.to().pos(), Class::First) / 1000.0)
        .sum::<f64>();
    let emissions_tons = Fact {
        claim: (commercial_emissions_tons * commercial_to_private_ratio) as usize,
        source: format!("Commercial flights would have emitted {commercial_emissions_tons:.1} tons of CO2e (based on [myclimate.org](https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/) - retrieved on 2023-10-19). Private jets emit 5-14x times. 10x was used based on [transportenvironment.org](https://www.transportenvironment.org/discover/private-jets-can-the-super-rich-supercharge-zero-emission-aviation/)"),
        date: "2023-10-05, from 2021-05-27".to_string(),
    };

    let short_legs = legs.iter().filter(|leg| leg.distance() < 300.0);
    let long_legs = legs.iter().filter(|leg| leg.distance() >= 300.0);

    let dane_emissions_tons = Fact {
            claim: 5.1,
            source: "A dane emitted 5.1 t CO2/person/year in 2019 according to [work bank data](https://ourworldindata.org/co2/country/denmark).".to_string(),
            date: "2023-10-08".to_string(),
        };

    let dane_years = format!(
        "{:.0}",
        emissions_tons.claim as f32 / dane_emissions_tons.claim as f32
    );
    let dane_years = Fact {
        claim: dane_years,
        source: "https://ourworldindata.org/co2/country/denmark Denmark emits 5.1 t CO2/person/year in 2019.".to_string(),
        date: "2023-10-08".to_string(),
    };

    let from_date = from.to_string();
    let to_date = to.to_string();

    let number_of_legs = Fact {
        claim: legs.len(),
        source: format!("[adsbexchange.com](https://globe.adsbexchange.com/?icao={icao}) between {from_date} and {to_date}"),
        date: to.to_string()
    };

    let context = Context {
        owner,
        aircraft,
        from_date,
        to_date,
        number_of_legs,
        emissions_tons,
        dane_years,
        number_of_legs_less_300km: short_legs.count(),
        number_of_legs_more_300km: long_legs.count(),
        ratio_commercial_300km: format!("{:.0}", commercial_to_private_ratio),
    };

    render(&context)?;

    Ok(())
}
