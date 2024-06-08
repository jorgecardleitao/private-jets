use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::Arc,
};

use itertools::Itertools;
use time::macros::date;

use crate::BlobStorageProvider;

/// Returns the complete set of (month, icao_number) for a given set of years and (optionally) countries.
/// This fetches the set of aircrafts available in the database in time, and joins (in time)
/// with the set of months in the requested years, effectively building the (time-dependent) dataset of all private jets.
///
/// ## Background
/// The set of aircrafts at a given point in time changes, as aircrafts are registered and deregistered to
/// allow registration numbers and icao numbers to be re-used.
///
/// This is reflected in db-exchange's database of "current" aircrafts.
/// The set of current aicrafts is snapshotted and operated by `crate::aircraft`.
///
/// ## Implementation
/// This function leverages these snapshots and the set of aircraft models to return the normalized set of
/// months, aircrafts.
pub async fn private_jets_in_month(
    years: impl Iterator<Item = i32>,
    maybe_country: Option<&str>,
    client: &dyn BlobStorageProvider,
) -> Result<HashSet<(Arc<str>, time::Date)>, Box<dyn Error>> {
    let models = crate::load_private_jet_models()?;
    let aircrafts = crate::aircraft::read_all(client).await?;

    // filter for private jet models and optionally country
    let private_jets = aircrafts
        .into_iter()
        .map(|(month, a)| {
            (
                month,
                // wrap in arc since we will point to this from from multiple months
                Arc::new(
                    a.into_iter()
                        .filter(|(_, a)| models.contains_key(&a.model))
                        .filter(|(_, a)| {
                            maybe_country
                                .map(|country| a.country.as_deref() == Some(country))
                                .unwrap_or(true)
                        })
                        .map(|(_, a)| a.icao_number.clone())
                        .collect::<HashSet<_>>(),
                ),
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
            let closest_set = private_jets.keys().fold(date!(1900 - 01 - 01), |a, b| {
                ((a - month).abs() < (*b - month).abs())
                    .then(|| a)
                    .unwrap_or(*b)
            });
            private_jets
                .get(&closest_set)
                .unwrap()
                .iter()
                .cloned()
                .map(move |icao| (icao, month.clone()))
        })
        .flatten()
        .collect::<HashSet<_>>();

    Ok(private_jets)
}
