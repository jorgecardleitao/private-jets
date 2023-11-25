use std::{collections::HashMap, error::Error, sync::Arc};

use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use simple_logger::SimpleLogger;

use flights::{emissions, load_aircraft_types, load_aircrafts, Class, Fact, Position};
use time::macros::date;

fn render(context: &Context) -> Result<(), Box<dyn Error>> {
    let path = "all_dk_jets.md";

    let template = std::fs::read_to_string("examples/dk_jets.md")?;

    let mut tt = tinytemplate::TinyTemplate::new();
    tt.set_default_formatter(&tinytemplate::format_unescaped);
    tt.add_template("t", &template)?;

    let rendered = tt.render("t", context)?;

    log::info!("Story written to {path}");
    std::fs::write(path, rendered)?;
    Ok(())
}

#[derive(serde::Serialize)]
pub struct Context {
    pub from_date: String,
    pub to_date: String,
    pub number_of_private_jets: Fact<usize>,
    pub number_of_legs: Fact<usize>,
    pub emissions_tons: Fact<usize>,
    pub dane_years: Fact<String>,
    pub number_of_legs_less_300km: usize,
    pub number_of_legs_more_300km: usize,
    pub ratio_commercial_300km: String,
}

#[derive(clap::ValueEnum, Debug, Clone)]
enum Backend {
    Disk,
    Azure,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// The Azure token
    #[arg(short, long)]
    azure_sas_token: Option<String>,
    #[arg(short, long, value_enum, default_value_t=Backend::Azure)]
    backend: Backend,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let cli = Cli::parse();

    // optionally initialize Azure client
    let client = match (cli.backend, cli.azure_sas_token) {
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
    let types = load_aircraft_types()?;

    let private_jets = aircrafts
        .into_iter()
        // is private jet
        .filter(|(_, a)| types.contains_key(&a.model))
        // is from DK
        .filter(|(a, _)| a.starts_with("OY-"))
        .collect::<HashMap<_, _>>();

    let number_of_private_jets = Fact {
        claim: private_jets.len(),
        source: format!(
            "All aircrafts in [adsbexchange.com](https://globe.adsbexchange.com) whose model is a private jet and tail number starts with \"OY-\""
        ),
        date: "2023-11-06".to_string(),
    };

    let to = time::OffsetDateTime::now_utc().date() - time::Duration::days(1);
    let from = date!(2021 - 01 - 01);

    let from_date = from.to_string();
    let to_date = to.to_string();

    let dates = flights::DateIter {
        from,
        to,
        increment: time::Duration::days(1),
    };

    let iter = dates
        .map(|date| {
            let client = client.as_ref();
            private_jets
                .iter()
                .map(move |(_, a)| flights::positions(&a.icao_number, date.clone(), 1000.0, client))
        })
        .flatten();

    let positions = futures::stream::iter(iter)
        // limit to 5 concurrent tasks
        .buffer_unordered(100)
        .try_collect::<Vec<_>>()
        .await?;
    let positions = positions.into_iter().flatten().collect::<Vec<_>>();

    // group by aircraft
    let mut positions = positions.into_iter().fold(
        HashMap::<Arc<str>, Vec<Position>>::default(),
        |mut acc, v| {
            acc.entry(v.icao().clone())
                .and_modify(|positions| positions.push(v.clone()))
                .or_insert_with(|| vec![v]);
            acc
        },
    );
    // sort positions by datetime
    positions.iter_mut().for_each(|(_, positions)| {
        positions.sort_unstable_by_key(|x| x.datetime());
    });

    // compute legs
    let legs = positions
        .into_iter()
        .map(|(icao, positions)| (icao, flights::real_legs(positions.into_iter())))
        .collect::<HashMap<_, _>>();

    let number_of_legs = Fact {
        claim: legs.iter().map(|(_, legs)| legs.len()).sum::<usize>(),
        source: format!("[adsbexchange.com](https://globe.adsbexchange.com) between {from_date} and {to_date} and all aircraft whose tail number starts with \"OY-\" and model is a private jet"),
        date: to.to_string()
    };

    let commercial_to_private_ratio = 10.0;
    let commercial_emissions_tons = legs
        .iter()
        .map(|(_, legs)| {
            legs.iter()
                .map(|leg| emissions(leg.from.pos(), leg.to.pos(), Class::First) / 1000.0)
                .sum::<f64>()
        })
        .sum::<f64>();
    let emissions_tons = Fact {
        claim: (commercial_emissions_tons * commercial_to_private_ratio) as usize,
        source: format!("Commercial flights would have emitted {commercial_emissions_tons:.1} tons of CO2e (based on [myclimate.org](https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/) - retrieved on 2023-10-19). Private jets emit 5-14x times. 10x was used based on [transportenvironment.org](https://www.transportenvironment.org/discover/private-jets-can-the-super-rich-supercharge-zero-emission-aviation/)"),
        date: "2023-10-05, from 2021-05-27".to_string(),
    };

    let short_legs = legs
        .iter()
        .map(|(_, legs)| legs.iter().filter(|leg| leg.distance() < 300.0).count())
        .sum::<usize>();
    let long_legs = legs
        .iter()
        .map(|(_, legs)| legs.iter().filter(|leg| leg.distance() >= 300.0).count())
        .sum::<usize>();

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

    let context = Context {
        from_date,
        to_date,
        number_of_private_jets,
        number_of_legs,
        emissions_tons,
        dane_years,
        number_of_legs_less_300km: short_legs,
        number_of_legs_more_300km: long_legs,
        ratio_commercial_300km: format!("{:.0}", commercial_to_private_ratio),
    };

    render(&context)?;

    Ok(())
}
