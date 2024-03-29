use std::{collections::HashMap, error::Error};

use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use num_format::{Locale, ToFormattedString};
use simple_logger::SimpleLogger;
use time::macros::date;

use flights::{
    aircraft, airports_cached, closest, emissions, leg_co2e_kg, leg_per_person,
    load_private_jet_models, AircraftModels, BlobStorageProvider, Class, Fact, Leg, LocalDisk,
    Position,
};
use time::Date;

static TEMPLATE: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/bin/country.md"));

fn render(context: &Context) -> Result<(), Box<dyn Error>> {
    let path = format!("{}_story.md", context.country.name.to_lowercase());

    let mut tt = tinytemplate::TinyTemplate::new();
    tt.set_default_formatter(&tinytemplate::format_unescaped);
    tt.add_template("t", TEMPLATE)?;

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
    from_airport: String,
    to_airport: String,
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
    Remote,
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
                claim: 8.2,
                source: "A dane emitted 8.2 t CO2e/person/year in 2019 according to [a 2021 European parliament brief on EU progress on climate action](https://www.europarl.europa.eu/RegData/etudes/BRIE/2021/679106/EPRS_BRI(2021)679106_EN.pdf)".to_string(),
                date: "2023-10-08".to_string(),
            },
            Country::Portugal => Fact {
                claim: 6.7,
                source: "A portuguese emitted 6.7 t CO2e/person/year in 2019 according to [a 2021 European parliament brief on EU progress on climate action](https://www.europarl.europa.eu/RegData/etudes/BRIE/2021/696196/EPRS_BRI(2021)696196_EN.pdf)".to_string(),
                date: "2024-01-23".to_string(),
            },
            Country::Spain => Fact {
                claim: 7.1,
                source: "A spanish emitted 7.1 t CO2e/person/year in 2019 according to [a 2021 European parliament brief on EU progress on climate action](https://www.europarl.europa.eu/RegData/etudes/BRIE/2021/690579/EPRS_BRI(2021)690579_EN.pdf)".to_string(),
                date: "2024-01-23".to_string(),
            },
            Country::Germany => Fact {
                claim: 10.1,
                source: "A german emitted 10.1 t CO2e/person/year in 2019 according to [a 2021 European parliament brief on EU progress on climate action](https://www.europarl.europa.eu/RegData/etudes/BRIE/2021/690661/EPRS_BRI(2021)690661_EN.pdf)".to_string(),
                date: "2024-01-23".to_string(),
            },
        }
    }
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
    client: &dyn BlobStorageProvider,
) -> Result<Vec<Leg>, Box<dyn Error>> {
    let positions = flights::aircraft_positions(from, to, icao_number, client).await?;

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
    legs: &HashMap<(String, String), Vec<Leg>>,
    models: &AircraftModels,
    filter: impl Fn(&&Leg) -> bool + Copy,
) -> f64 {
    legs.iter()
        .map(|((_, model), legs)| {
            legs.iter()
                .filter(filter)
                .map(|leg| {
                    leg_co2e_kg(models.get(model).expect(model).gph as f64, leg.duration()) / 1000.0
                })
                .sum::<f64>()
        })
        .sum::<f64>()
}

fn commercial_emissions(
    legs: &HashMap<(String, String), Vec<Leg>>,
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
    let aircrafts = aircraft::read(date!(2023 - 11 - 06), client).await?;
    let models = load_private_jet_models()?;
    let airports = airports_cached().await?;

    let private_jets = aircrafts
        .into_iter()
        // is private jet
        .filter(|(_, a)| models.contains_key(&a.model))
        // from country
        .filter(|(a, _)| a.starts_with(cli.country.tail_number()))
        .collect::<HashMap<_, _>>();

    let now = time::OffsetDateTime::now_utc().date();
    let from = cli.from;
    let to = cli.to.unwrap_or(now);

    let from_date = from.to_string();
    let to_date = to.to_string();

    let legs = private_jets.iter().map(|(_, aircraft)| async {
        legs(from, to, &aircraft.icao_number, cli.location, client)
            .await
            .map(|legs| ((aircraft.tail_number.clone(), aircraft.model.clone()), legs))
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
                from_airport: closest(leg.from().pos(), &airports).name,
                to_airport: closest(leg.to().pos(), &airports).name,
                from_lat: leg.from().latitude(),
                from_lon: leg.from().longitude(),
                to_lat: leg.to().latitude(),
                to_lon: leg.to().longitude(),
                commercial_emissions_kg: emissions(leg.from().pos(), leg.to().pos(), Class::First)
                    as usize,
                emissions_kg: leg_co2e_kg(
                    models.get(model).expect(model).gph as f64,
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

    let emissions_value_tons = private_emissions(&legs, &models, |_| true);

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

    let emissions_short_legs = private_emissions(&legs, &models, |leg| leg.distance() < 300.0);
    let commercial_emissions_short = commercial_emissions(&legs, |leg| leg.distance() < 300.0);

    let short_ratio = leg_per_person(emissions_short_legs) / commercial_emissions_short;
    let ratio_train_300km = Fact {
        claim: (short_ratio + 7.0) as usize,
        source: format!("{}x in comparison to a commercial flight[^1][^6] plus 7x of a commercial flight in comparison to a train, as per https://ourworldindata.org/travel-carbon-footprint (UK data, vary by country) - retrieved on 2024-01-20", short_ratio as usize),
        date: now.to_string()
    };

    // compute emissions for the >300km legs, so we can compare with emissions from commercial flights
    let emissions_long_legs = private_emissions(&legs, &models, |leg| leg.distance() >= 300.0);
    let commercial_emissions_long = commercial_emissions(&legs, |leg| leg.distance() >= 300.0);

    let ratio_commercial_300km = Fact {
        claim: (leg_per_person(emissions_long_legs) / commercial_emissions_long) as usize,
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
