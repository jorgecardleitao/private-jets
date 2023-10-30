use std::error::Error;

use tinytemplate::TinyTemplate;

use flights::*;

use clap::Parser;

static TEMPLATE_NAME: &'static str = "t";

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// The tail number
    #[arg(short, long)]
    tail_number: String,
    /// The date
    #[arg(short, long)]
    date: String,

    /// The cookie retrieved from https://globe.adsbexchange.com/. Something like "adsbx_sid=1697996994839_e9zejgp1o; adsbx_api=1697997662491_tl8d1cpxfvi"
    #[arg(short, long)]
    cookie: String,
}

pub fn flight_date(
    tail_number: &str,
    date: &str,
    cookie: &str,
    owners: &Owners,
    aircrafts: &Aircrafts,
) -> Result<Vec<Event>, Box<dyn Error>> {
    let airports = airports_cached()?;
    let to_icao = number_to_icao()?;
    let specifications = load_specification()?;
    let aircraft = aircrafts
        .get(tail_number)
        .ok_or_else(|| Into::<Box<dyn Error>>::into("Tail number not found"))?;
    let company = owners
        .get(&aircraft.owner)
        .ok_or_else(|| Into::<Box<dyn Error>>::into("Owner not found"))?;
    let owner = Fact {
        claim: company.clone(),
        source: aircraft.source.clone(),
        date: aircraft.date.clone(),
    };

    println!("Owner found: {}", owner.claim.name);
    let normalized_tail = tail_number.replace("-", "");
    let icao = to_icao.get(&normalized_tail).unwrap().to_ascii_lowercase();
    println!("ICAO found: {}", icao);
    let legs = legs(&icao, date, cookie)?;
    println!("Legs: {}", legs.len());

    Ok(legs.into_iter().filter_map(|leg| {
        let is_leg = matches!(leg.from, Position::Grounded(_, _, _)) & matches!(leg.to, Position::Grounded(_, _, _));
        if !is_leg {
            println!("{:?} -> {:?} skipped", leg.from, leg.to);
        }
        is_leg.then_some((leg.from, leg.to))
    }).map(|(from, to)| {
        let emissions = emissions(from.pos(), to.pos(), Class::First);
        let emissions_private = emissions_private_jet(&aircraft.model, from, to, &specifications);

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
                emissions_kg: emissions_private,
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

    let template = std::fs::read_to_string("src/template.md")?;

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
    let aircrafts = load_aircrafts()?;

    let dane_emissions_kg = Fact {
        claim: 5100,
        source: "https://ourworldindata.org/co2/country/denmark Denmark emits 5.1 t CO2/person/year in 2019.".to_string(),
        date: "2023-10-08".to_string(),
    };

    let mut events = flight_date(
        &cli.tail_number,
        &cli.date,
        &cli.cookie,
        &owners,
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
