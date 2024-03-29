static LITER_PER_GALON: f64 = 3.78541;
static KG_PER_LITER: f64 = 0.8;
static EMISSIONS_PER_KG: f64 = 3.16;
static RADIATIVE_INDEX: f64 = 3.0;
static LIFE_CYCLE_FACTOR: f64 = 1.68;
static OCCUPANCY_FACTOR: f64 = 0.23;

/// Returns the total CO2e emissions in kg of a private jet with a given
/// consumption (in GPH) of Jet-A fuel flying for a given amount of time,
/// as specified in [methodology `M-7`](../methodology.md).
pub fn leg_co2e_kg(consumption: f64, duration: time::Duration) -> f64 {
    let hours = duration.as_seconds_f64() / 60.0 / 60.0;
    consumption
        * hours
        * LITER_PER_GALON
        * KG_PER_LITER
        * EMISSIONS_PER_KG
        * RADIATIVE_INDEX
        * LIFE_CYCLE_FACTOR
}

/// Returns the total emissions per person of a private jet with a given
/// consumption (in GPH) of Jet-A fuel flying for a given amount of time,
/// as specified in [methodology `M-7`](../methodology.md).
pub fn leg_per_person(emissions: f64) -> f64 {
    emissions * OCCUPANCY_FACTOR
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basics() {
        assert_eq!(
            leg_co2e_kg(280.0, time::Duration::hours(2)),
            27009.003313152003
        );
    }
}
