use std::error::Error;

use flights::{
    emissions, load_aircraft_owners, load_aircrafts, load_owners, Aircraft, Class, Company, Fact,
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

fn render(context: &Context) -> Result<(), Box<dyn Error>> {
    let path = "story.md";

    let template = std::fs::read_to_string("examples/period_template.md")?;

    let mut tt = tinytemplate::TinyTemplate::new();
    tt.set_default_formatter(&tinytemplate::format_unescaped);
    tt.add_template("t", &template)?;

    let rendered = tt.render("t", context)?;

    println!("Story written to {path}");
    std::fs::write(path, rendered)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let owners = load_owners()?;
    let aircraft_owners = load_aircraft_owners()?;
    let aircrafts = load_aircrafts()?;

    let to = time::OffsetDateTime::now_utc().date() - time::Duration::days(1);
    let from = to - time::Duration::days(90);

    let tail_number = "OY-GFS";
    let aircraft = aircrafts
        .get(tail_number)
        .ok_or_else(|| Into::<Box<dyn Error>>::into("Aircraft ICAO number not found"))?
        .clone();
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

    let icao = &aircraft.icao_number;
    println!("ICAO number: {}", icao);

    let iter = flights::DateIter {
        from,
        to,
        increment: time::Duration::days(1),
    };

    let mut positions = vec![];
    for date in iter {
        positions.extend(flights::positions(icao, &date, 1000.0)?);
    }

    let legs = flights::legs(positions.into_iter());
    let legs = legs
        .into_iter()
        // ignore legs that are too fast, as they are likely noise
        .filter(|leg| leg.duration() > time::Duration::minutes(5))
        // ignore legs that are too short, as they are likely noise
        .filter(|leg| leg.distance() > 3.0)
        // ignore legs that are too low, as they are likely noise
        .filter(|leg| leg.maximum_altitude > 1000.0)
        .collect::<Vec<_>>();
    println!("number_of_legs: {}", legs.len());
    for leg in &legs {
        println!(
            "{},{},{},{},{},{},{},{},{}",
            leg.from.datetime(),
            leg.from.latitude(),
            leg.from.longitude(),
            leg.from.altitude(),
            leg.to.datetime(),
            leg.to.latitude(),
            leg.to.longitude(),
            leg.to.altitude(),
            leg.maximum_altitude
        );
    }

    let commercial_to_private_ratio = 10.0;
    let commercial_emissions_tons = legs
        .iter()
        .map(|leg| emissions(leg.from.pos(), leg.to.pos(), Class::First) / 1000.0)
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
