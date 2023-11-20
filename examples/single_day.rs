use std::error::Error;

use tinytemplate::TinyTemplate;

use flights::*;

use clap::Parser;

static TEMPLATE_NAME: &'static str = "t";

#[derive(serde::Serialize, serde::Deserialize, Debug)]
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

#[derive(serde::Serialize)]
pub struct Context {
    pub event: Event,
    pub dane_emissions_kg: Fact<usize>,
    pub dane_years: String,
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// The tail number
    #[arg(short, long)]
    tail_number: String,
    /// The date in format `yyyy-mm-dd`
    #[arg(short, long)]
    date: String,
}

pub fn flight_date(
    tail_number: &str,
    date: &time::Date,
    owners: &Owners,
    aircraft_owners: &AircraftOwners,
    aircrafts: &Aircrafts,
) -> Result<Vec<Event>, Box<dyn Error>> {
    let airports = airports_cached()?;
    let aircraft_owner = aircraft_owners
        .get(tail_number)
        .ok_or_else(|| Into::<Box<dyn Error>>::into("Owner of tail number not found"))?;
    println!("Aircraft owner: {}", aircraft_owner.owner);
    let company = owners
        .get(&aircraft_owner.owner)
        .ok_or_else(|| Into::<Box<dyn Error>>::into("Owner not found"))?;
    println!("Owner information found");
    let owner = Fact {
        claim: company.clone(),
        source: aircraft_owner.source.clone(),
        date: aircraft_owner.date.clone(),
    };

    let aircraft = aircrafts
        .get(tail_number)
        .ok_or_else(|| Into::<Box<dyn Error>>::into("Aircraft ICAO number not found"))?;
    let icao = &aircraft.icao_number;
    println!("ICAO number: {}", icao);

    let positions = positions(icao, date, 1000.0)?;
    let legs = legs(positions);

    println!("Number of legs: {}", legs.len());

    Ok(legs.into_iter().filter_map(|leg| {
        let is_leg = matches!(leg.from, Position::Grounded{..}) & matches!(leg.to, Position::Grounded{..});
        if !is_leg {
            println!("{:?} -> {:?} skipped", leg.from, leg.to);
        }
        is_leg.then_some((leg.from, leg.to))
    }).map(|(from, to)| {
        let emissions = emissions(from.pos(), to.pos(), Class::First);

        Event {
            tail_number: tail_number.to_string(),
                owner: owner.clone(),
                date: date.to_string(),
                from_airport: closest(from.pos(), &airports).name.clone(),
                to_airport: closest(to.pos(), &airports).name.clone(),
                two_way: false,
                commercial_emissions_kg: Fact {
                    claim: emissions as usize,
                    source: "https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/".to_string(),
                    date: "2023-10-19".to_string()
                },
                emissions_kg: Fact {
                    claim: (emissions * 10.0) as usize,
                    source: "Private jets emit 5-14x times. 10x was used here https://www.transportenvironment.org/discover/private-jets-can-the-super-rich-supercharge-zero-emission-aviation/".to_string(),
                    date: "2023-10-05, from 2021-05-27".to_string(),
                },
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

    let template = std::fs::read_to_string("examples/single_day_template.md")?;

    let mut tt = TinyTemplate::new();
    tt.set_default_formatter(&tinytemplate::format_unescaped);
    tt.add_template(TEMPLATE_NAME, &template)?;

    let rendered = tt.render(TEMPLATE_NAME, &context)?;

    println!("Story written to {path}");
    std::fs::write(path, rendered)?;

    Ok(())
}

pub fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    std::fs::create_dir_all("database")?;

    let owners = load_owners()?;
    let aircraft_owners = load_aircraft_owners()?;
    let aircrafts = load_aircrafts()?;

    let dane_emissions_kg = Fact {
        claim: 5100,
        source: "https://ourworldindata.org/co2/country/denmark Denmark emits 5.1 t CO2/person/year in 2019.".to_string(),
        date: "2023-10-08".to_string(),
    };

    let date = time::Date::parse(
        &cli.date,
        time::macros::format_description!("[year]-[month]-[day]"),
    )?;

    let mut events = flight_date(
        &cli.tail_number,
        &date,
        &owners,
        &aircraft_owners,
        &aircrafts,
    )?;

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
