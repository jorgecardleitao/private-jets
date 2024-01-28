use std::{collections::HashMap, error::Error, sync::Arc};

use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use num_format::{Locale, ToFormattedString};
use simple_logger::SimpleLogger;

use flights::{
    emissions, leg_co2_kg, load_aircraft_consumption, load_aircrafts, load_private_jet_types,
    AircraftTypeConsumptions, Class, Fact, Leg, Position,
};
use time::Date;

fn render(context: &Context) -> Result<(), Box<dyn Error>> {
    let path = format!("{}_story.md", context.country.name.to_lowercase());

    let template = std::fs::read_to_string("examples/country.md")?;

    let mut tt = tinytemplate::TinyTemplate::new();
    tt.set_default_formatter(&tinytemplate::format_unescaped);
    tt.add_template("t", &template)?;

    let rendered = tt.render("t", context)?;

    log::info!("Story written to {path}");
    std::fs::write(path, rendered)?;
    Ok(())
}

#[derive(serde::Serialize)]
struct LegOut {
    tail_number: String,
    model: String,
    start: String,
    end: String,
    duration: String,
    from_lat: f64,
    from_lon: f64,
    to_lat: f64,
    to_lon: f64,
    commercial_emissions_kg: usize,
    emissions_kg: usize,
}

#[derive(serde::Serialize)]
pub struct CountryContext {
    pub name: String,
    pub plural: String,
    pub possessive: String,
}

#[derive(serde::Serialize)]
pub struct Context {
    pub country: CountryContext,
    pub location: String,
    pub from_date: String,
    pub to_date: String,
    pub number_of_private_jets: Fact<String>,
    pub number_of_legs: Fact<String>,
    pub emissions_tons: Fact<String>,
    pub citizen_years: Fact<String>,
    pub number_of_legs_less_300km: String,
    pub number_of_legs_more_300km: String,
    pub ratio_commercial_300km: Fact<usize>,
    pub ratio_train_300km: Fact<usize>,
}

#[derive(clap::ValueEnum, Debug, Clone)]
enum Backend {
    Disk,
    Azure,
}

fn parse_date(arg: &str) -> Result<time::Date, time::error::Parse> {
    time::Date::parse(
        arg,
        time::macros::format_description!("[year]-[month]-[day]"),
    )
}

#[derive(clap::ValueEnum, Debug, Clone)]
enum Country {
    Denmark,
    Portugal,
    Spain,
    Germany,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
enum Location {
    Davos,
}

impl Location {
    fn name(&self) -> &'static str {
        match self {
            Self::Davos => "Davos airport (LSZR)",
        }
    }

    fn region(&self) -> [[f64; 2]; 2] {
        match self {
            Self::Davos => [[47.482, 9.538], [47.490, 9.568]],
        }
    }
}

impl Country {
    fn to_context(&self) -> CountryContext {
        CountryContext {
            name: self.name().to_string(),
            plural: self.plural().to_string(),
            possessive: self.possessive().to_string(),
        }
    }

    fn possessive(&self) -> &'static str {
        match self {
            Self::Denmark => "Danish",
            Self::Portugal => "Portuguese",
            Self::Spain => "Spanish",
            Self::Germany => "German",
        }
    }

    fn plural(&self) -> &'static str {
        match self {
            Self::Denmark => "Danes",
            Self::Portugal => "Portugueses",
            Self::Spain => "Spanish",
            Self::Germany => "Germans",
        }
    }

    fn tail_number(&self) -> &'static str {
        match self {
            Self::Denmark => "OY-",
            Self::Portugal => "CS-",
            Self::Spain => "EC-",
            Self::Germany => "D-",
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Country::Denmark => "Denmark",
            Country::Portugal => "Portugal",
            Country::Spain => "Spain",
            Country::Germany => "Germany",
        }
    }

    fn emissions(&self) -> Fact<f64> {
        match self {
            Country::Denmark => Fact {
                claim: 5.1,
                source: "A dane emitted 5.1 t CO2/person/year in 2019 according to [work bank data](https://ourworldindata.org/co2/country/denmark).".to_string(),
                date: "2023-10-08".to_string(),
            },
            Country::Portugal => Fact {
                claim: 4.1,
                source: "A portuguese emitted 4.1 t CO2/person/year in 2022 according to [work bank data](https://ourworldindata.org/co2/country/denmark).".to_string(),
                date: "2024-01-23".to_string(),
            },
            Country::Spain => Fact {
                claim: 5.2,
                source: "A spanish emitted 5.2 t CO2/person/year in 2022 according to [work bank data](https://ourworldindata.org/co2/country/spain).".to_string(),
                date: "2024-01-23".to_string(),
            },
            Country::Germany => Fact {
                claim: 8.0,
                source: "A german emitted 8.0 t CO2/person/year in 2022 according to [work bank data](https://ourworldindata.org/co2/country/germany).".to_string(),
                date: "2024-01-23".to_string(),
            },
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// The Azure token
    #[arg(long)]
    azure_sas_token: Option<String>,
    #[arg(long, value_enum, default_value_t=Backend::Azure)]
    backend: Backend,

    /// Name of the country to compute on
    #[arg(long)]
    country: Country,

    /// A date in format `yyyy-mm-dd`
    #[arg(long, value_parser = parse_date)]
    from: time::Date,
    /// Optional end date in format `yyyy-mm-dd` (else it is to today)
    #[arg(long, value_parser = parse_date)]
    to: Option<time::Date>,

    /// Optional location to restrict the search geographically. Currently only
    #[arg(long)]
    location: Option<Location>,
}

pub fn in_box(position: &Position, region: [[f64; 2]; 2]) -> bool {
    return (position.latitude() >= region[0][0] && position.latitude() < region[1][0])
        && (position.longitude() >= region[0][1] && position.longitude() < region[1][1]);
}

async fn legs(
    from: Date,
    to: Date,
    icao_number: &str,
    location: Option<Location>,
    client: Option<&flights::fs_azure::ContainerClient>,
) -> Result<Vec<Leg>, Box<dyn Error>> {
    let positions = flights::aircraft_positions(from, to, icao_number, client).await?;
    let mut positions = positions
        .into_iter()
        .map(|(_, p)| p)
        .flatten()
        .collect::<Vec<_>>();
    positions.sort_unstable_by_key(|p| p.datetime());

    log::info!("Computing legs {}", icao_number);
    let legs = flights::legs(positions.into_iter());

    // filter by location
    if let Some(location) = location {
        let region = location.region();
        Ok(legs
            .into_iter()
            .filter(|leg| leg.positions().iter().any(|p| in_box(p, region)))
            .collect())
    } else {
        Ok(legs)
    }
}

fn private_emissions(
    legs: &HashMap<(Arc<str>, String), Vec<Leg>>,
    consumptions: &AircraftTypeConsumptions,
    filter: impl Fn(&&Leg) -> bool + Copy,
) -> f64 {
    legs.iter()
        .map(|((_, model), legs)| {
            legs.iter()
                .filter(filter)
                .map(|leg| {
                    leg_co2_kg(
                        consumptions.get(model).expect(model).gph as f64,
                        leg.duration(),
                    ) / 1000.0
                })
                .sum::<f64>()
        })
        .sum::<f64>()
}

fn commercial_emissions(
    legs: &HashMap<(Arc<str>, String), Vec<Leg>>,
    filter: impl Fn(&&Leg) -> bool + Copy,
) -> f64 {
    legs.iter()
        .map(|(_, legs)| {
            legs.iter()
                .filter(filter)
                .map(|leg| emissions(leg.from().pos(), leg.to().pos(), Class::First) / 1000.0)
                .sum::<f64>()
        })
        .sum::<f64>()
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
    let types = load_private_jet_types()?;
    let consumptions = load_aircraft_consumption()?;

    let private_jets = aircrafts
        .into_iter()
        // is private jet
        .filter(|(_, a)| types.contains_key(&a.model))
        // from country
        .filter(|(a, _)| a.starts_with(cli.country.tail_number()))
        .collect::<HashMap<_, _>>();

    let now = time::OffsetDateTime::now_utc().date();
    let from = cli.from;
    let to = cli.to.unwrap_or(now);

    let from_date = from.to_string();
    let to_date = to.to_string();

    let client = client.as_ref();
    let legs = private_jets.iter().map(|(_, aircraft)| async {
        legs(from, to, &aircraft.icao_number, cli.location, client)
            .await
            .map(|legs| ((aircraft.icao_number.clone(), aircraft.model.clone()), legs))
    });

    let legs = futures::stream::iter(legs)
        // limit concurrent tasks
        .buffered(1)
        .try_collect::<HashMap<_, _>>()
        .await?;

    let mut wtr = csv::Writer::from_writer(vec![]);
    for ((tail_number, model), legs) in legs.iter() {
        for leg in legs {
            wtr.serialize(LegOut {
                tail_number: tail_number.to_string(),
                model: model.to_string(),
                start: leg.from().datetime().to_string(),
                end: leg.to().datetime().to_string(),
                duration: leg.duration().to_string(),
                from_lat: leg.from().latitude(),
                from_lon: leg.from().longitude(),
                to_lat: leg.to().latitude(),
                to_lon: leg.to().longitude(),
                commercial_emissions_kg: emissions(leg.from().pos(), leg.to().pos(), Class::First)
                    as usize,
                emissions_kg: leg_co2_kg(
                    consumptions.get(model).expect(model).gph as f64,
                    leg.duration(),
                ) as usize,
            })
            .unwrap()
        }
    }
    let data_csv = wtr.into_inner().unwrap();
    std::fs::write("data.csv", data_csv)?;

    let number_of_private_jets = Fact {
        claim: legs.iter().filter(|x| x.1.len() > 0).count().to_formatted_string(&Locale::en),
        source: format!(
            "All aircrafts in [adsbexchange.com](https://globe.adsbexchange.com) whose model is a private jet, registered in {}, and with at least one leg - ", cli.country.name()
        ),
        date: "2023-11-06".to_string(),
    };

    let number_of_legs = Fact {
        claim: legs
            .iter()
            .map(|(_, legs)| legs.len())
            .sum::<usize>()
            .to_formatted_string(&Locale::en),
        source: format!(
            "[adsbexchange.com](https://globe.adsbexchange.com) between {from_date} and {to_date}"
        ),
        date: now.to_string(),
    };

    let emissions_value_tons = private_emissions(&legs, &consumptions, |_| true);

    let emissions_tons = Fact {
        claim: (emissions_value_tons as usize).to_formatted_string(&Locale::en),
        source: "See [methodology M-7](https://github.com/jorgecardleitao/private-jets/blob/main/methodology.md)".to_string(),
        date: time::OffsetDateTime::now_utc().date().to_string(),
    };

    let short_legs = legs
        .iter()
        .map(|(_, legs)| legs.iter().filter(|leg| leg.distance() < 300.0).count())
        .sum::<usize>();
    let long_legs = legs
        .iter()
        .map(|(_, legs)| legs.iter().filter(|leg| leg.distance() >= 300.0).count())
        .sum::<usize>();

    let emissions_short_legs =
        private_emissions(&legs, &consumptions, |leg| leg.distance() < 300.0);
    let commercial_emissions_short = commercial_emissions(&legs, |leg| leg.distance() < 300.0);

    let short_ratio = emissions_short_legs / commercial_emissions_short;
    let ratio_train_300km = Fact {
        claim: (short_ratio + 7.0) as usize,
        source: format!("{}x in comparison to a commercial flight[^1][^6] plus 7x of a commercial flight in comparison to a train, as per https://ourworldindata.org/travel-carbon-footprint (UK data, vary by country) - retrieved on 2024-01-20", short_ratio as usize),
        date: now.to_string()
    };

    // compute emissions for the >300km legs, so we can compare with emissions from commercial flights
    let emissions_long_legs =
        private_emissions(&legs, &consumptions, |leg| leg.distance() >= 300.0);
    let commercial_emissions_long = commercial_emissions(&legs, |leg| leg.distance() >= 300.0);

    let ratio_commercial_300km = Fact {
        claim: (emissions_long_legs / commercial_emissions_long) as usize,
        source: "Commercial flight emissions based on [myclimate.org](https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/) - retrieved on 2023-10-19".to_string(),
        date: now.to_string(),
    };

    let citizen_emissions_tons = cli.country.emissions();

    let citizen_years = (emissions_value_tons / citizen_emissions_tons.claim) as usize;
    let citizen_years = Fact {
        claim: citizen_years.to_formatted_string(&Locale::en),
        source: citizen_emissions_tons.source,
        date: citizen_emissions_tons.date,
    };

    let context = Context {
        country: cli.country.to_context(),
        location: cli
            .location
            .map(|l| format!(" at {}", l.name()))
            .unwrap_or_default(),
        from_date,
        to_date,
        number_of_private_jets,
        number_of_legs,
        emissions_tons,
        citizen_years,
        number_of_legs_less_300km: short_legs.to_formatted_string(&Locale::en),
        number_of_legs_more_300km: long_legs.to_formatted_string(&Locale::en),
        ratio_commercial_300km,
        ratio_train_300km,
    };

    render(&context)?;

    Ok(())
}
