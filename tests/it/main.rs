use std::error::Error;

use time::macros::date;

/// Verifies that we compute the correct number of legs.
/// The expected 2 was confirmed by manual inspection of
/// https://globe.adsbexchange.com/?icao=45d2ed&lat=54.128&lon=9.185&zoom=5.0&showTrace=2023-10-13
#[test]
fn acceptance_legs() -> Result<(), Box<dyn Error>> {
    let positions = flights::positions("45d2ed", &date!(2023 - 10 - 13), 1000.0)?;
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
