use std::error::Error;

use flights::Leg;
use time::{macros::date, Date};

/// Verifies that we compute the correct number of legs.
/// The expected 2 was confirmed by manual inspection of
/// https://globe.adsbexchange.com/?icao=45d2ed&lat=54.128&lon=9.185&zoom=5.0&showTrace=2023-10-13
#[tokio::test]
async fn acceptance_legs() -> Result<(), Box<dyn Error>> {
    let positions = flights::positions("45d2ed", date!(2023 - 10 - 13), None).await?;
    let legs = flights::legs(positions);

    assert_eq!(legs.len(), 2);

    Ok(())
}

fn abs_difference<T: std::ops::Sub<Output = T> + PartialOrd>(x: T, y: T) -> T {
    if x < y {
        y - x
    } else {
        x - y
    }
}

/// Verifies that `emissions` yields the same result as
/// https://co2.myclimate.org/en/flight_calculators/new
/// thereby establishing that it correctly implements the calculation at
/// https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/
#[test]
fn acceptance_test_emissions() {
    let berlin = (52.3650, 13.5010);
    let brussels = (50.9008, 4.4865);

    let accepted_error = 0.01; // 1%

    // From: Berlin (DE), BER to: Brussels (BE), BRU, One way, Business Class, ca. 600 km, 1 traveller
    let expected = 0.215 * 1000.0;
    let emissions = flights::emissions(berlin, brussels, flights::Class::Business);
    assert!(abs_difference(emissions, expected) / expected < accepted_error);

    // From: Berlin (DE), BER to: Brussels (BE), BRU, One way, First Class, ca. 600 km, 1 traveller
    let expected = 0.398 * 1000.0;
    let emissions = flights::emissions(berlin, brussels, flights::Class::First);
    assert!(abs_difference(emissions, expected) / expected < accepted_error);
}

#[tokio::test]
async fn legs_() -> Result<(), Box<dyn Error>> {
    let positions = flights::positions("459cd3", date!(2023 - 11 - 17), None).await?;
    let legs = flights::legs(positions);

    // same as ads-b computes: https://globe.adsbexchange.com/?icao=459cd3&lat=53.265&lon=8.038&zoom=6.5&showTrace=2023-11-17
    assert_eq!(legs.len(), 5);
    Ok(())
}

async fn legs(
    from: Date,
    to: Date,
    icao_number: &str,
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
    Ok(legs)
}

#[tokio::test]
async fn multi_day_legs() -> Result<(), Box<dyn Error>> {
    let legs = legs(date!(2023 - 07 - 21), date!(2023 - 07 - 23), "458d90", None).await?;

    // same as ads-b computes: https://globe.adsbexchange.com/?icao=458d90&lat=53.265&lon=8.038&zoom=6.5&showTrace=2023-07-21
    assert_eq!(legs.len(), 6);
    Ok(())
}

#[tokio::test]
async fn fs_azure() -> Result<(), Box<dyn Error>> {
    let client = flights::fs_azure::initialize_anonymous("privatejets", "data");

    let _ = flights::positions("459cd3", date!(2020 - 01 - 01), Some(&client)).await?;
    Ok(())
}

#[tokio::test]
async fn case_459257_2023_12_17() -> Result<(), Box<dyn Error>> {
    let legs = legs(date!(2023 - 12 - 17), date!(2023 - 12 - 20), "459257", None).await?;
    assert_eq!(legs.len(), 4);
    Ok(())
}

/// Case of losing signal for 2 days mid flight.
/// https://globe.adsbexchange.com/?icao=45dd84&lat=9.613&lon=22.035&zoom=3.8&showTrace=2023-12-08
#[tokio::test]
async fn case_45dd84_2023_12_06() -> Result<(), Box<dyn Error>> {
    let legs = legs(date!(2023 - 12 - 06), date!(2023 - 12 - 09), "45dd84", None).await?;
    assert_eq!(legs.len(), 3);
    let day = 24.0 * 60.0 * 60.0;
    assert!(legs[0].duration().as_seconds_f32() < day);
    assert!(legs[1].duration().as_seconds_f32() < day);
    assert!(legs[2].duration().as_seconds_f32() < day);
    Ok(())
}

#[tokio::test]
async fn case_45c824_2023_12_12() -> Result<(), Box<dyn Error>> {
    let legs = legs(date!(2023 - 12 - 12), date!(2023 - 12 - 16), "45c824", None).await?;

    assert_eq!(legs.len(), 3);
    let day = 24.0 * 60.0 * 60.0;
    assert!(legs[0].duration().as_seconds_f32() < day);
    assert!(legs[1].duration().as_seconds_f32() < day);
    assert!(legs[2].duration().as_seconds_f32() < day);
    Ok(())
}
