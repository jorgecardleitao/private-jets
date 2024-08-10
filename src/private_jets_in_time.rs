use std::{collections::HashMap, error::Error, sync::Arc};

use itertools::Itertools;
use time::macros::date;
use time::Date;

use crate::{aircraft::Aircraft, fs::BlobStorageProvider, model::AircraftModel};

pub type RequiredTasks = HashMap<(Arc<str>, time::Date), (Arc<Aircraft>, Arc<AircraftModel>)>;

/// Returns the map `(icao_number, month) -> `[`Aircraft`] for the given set of years and (optionally) countries.
/// The key is the specific `(icao_number, month)`, the value is the [`Aircraft`] associated with that icao_number at that month.
///
/// ## Background
/// The association between an `icao_number` and an [`Aircraft`] (tail number, model, etc.) at a given point in time changes,
/// as aircrafts are registered and deregistered to allow registration numbers and icao numbers to be re-used.
///
/// This is reflected in db-exchange's database of "current" aircrafts.
/// The set of current aicrafts is snapshotted for different times by `crate::aircraft`.
///
/// This function uses these snapshots to construct the time-dependent map between icao numbers and [`Aircraft`].
///
/// ## Implementation
/// This function fetches the set of aircrafts available in the database in time, and joins (in time) with the set of months in the requested years.
///
/// It leverages these snapshots and the set of aircraft models to return the normalized set of months, aircrafts.
pub async fn private_jets_in_month(
    years: impl Iterator<Item = i32>,
    maybe_country: Option<&str>,
    client: &dyn BlobStorageProvider,
) -> Result<RequiredTasks, Box<dyn Error>> {
    let models = crate::model::load_private_jet_models()?;
    let aircrafts = crate::aircraft::read_all(client).await?;

    // set of icao numbers that are private jets, for each date
    let private_jets = aircrafts
        .into_iter()
        .map(|(date, a)| {
            (
                date,
                a.into_iter()
                    // filter by optional country
                    .filter(|(_, a)| {
                        maybe_country
                            .map(|country| a.country.as_deref() == Some(country))
                            .unwrap_or(true)
                    })
                    // filter for private jet models and optionally country
                    .filter_map(|(icao_number, a)| {
                        models
                            .get(&a.model)
                            .map(|m| (icao_number, (Arc::new(a), m.clone())))
                    })
                    .collect::<HashMap<_, _>>(),
            )
        })
        .collect::<HashMap<_, _>>();

    // set of all months for requested years
    let now = time::OffsetDateTime::now_utc().date();
    let now =
        time::Date::from_calendar_date(now.year(), now.month(), 1).expect("day 1 never errors");
    let months = years
        .cartesian_product(1..=12u8)
        .map(|(year, month)| {
            time::Date::from_calendar_date(year, time::Month::try_from(month).unwrap(), 1)
                .expect("day 1 never errors")
        })
        .filter(|month| month < &now);

    // for each month, get the list of private jets closest from the start of month
    let private_jets = months
        .map(|month| {
            let closest_date = closest_date(private_jets.keys().copied(), month);
            private_jets
                .get(&closest_date)
                .unwrap()
                .iter()
                .map(move |(icao, aircraft)| ((icao.clone(), month), aircraft.clone()))
        })
        .flatten()
        .collect::<HashMap<_, _>>();

    Ok(private_jets)
}

fn closest_date(dates: impl Iterator<Item = Date>, target: Date) -> Date {
    dates.fold(date!(1900 - 01 - 01), |a, b| {
        ((a - target).abs() < (b - target).abs())
            .then(|| a)
            .unwrap_or(b)
    })
}

#[cfg(test)]
mod test {
    use time::macros::date;

    use super::*;

    #[test]
    fn test_closest_date() {
        assert_eq!(
            closest_date(
                vec![date!(2022 - 02 - 01), date!(2010 - 02 - 01)].into_iter(),
                date!(2023 - 02 - 01)
            ),
            date!(2022 - 02 - 01)
        );

        assert_eq!(
            closest_date(
                vec![date!(2022 - 02 - 01), date!(2010 - 02 - 01)].into_iter(),
                date!(2011 - 02 - 01)
            ),
            date!(2010 - 02 - 01)
        );
    }
}
