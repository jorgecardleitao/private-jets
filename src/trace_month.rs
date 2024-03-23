use std::{collections::HashSet, error::Error, sync::Arc};

use futures::{StreamExt, TryStreamExt};
use time::Date;

use super::Position;
use crate::{cached_aircraft_positions, fs, BlobStorageProvider};

static DATABASE: &'static str = "position/";

fn blob_name_to_pk(prefix: &str, blob: &str) -> (Arc<str>, time::Date) {
    let bla = &blob[prefix.len() + "icao_number=".len()..];
    let end = bla.find("/").unwrap();
    let icao = &bla[..end];
    let date_start = end + "/month=".len();
    let date = &bla[date_start..date_start + "2024-01".len()];
    (
        icao.into(),
        time::Date::from_calendar_date(
            date[..4].parse().unwrap(),
            date[5..7]
                .parse::<u8>()
                .expect(&date[5..7])
                .try_into()
                .unwrap(),
            1,
        )
        .unwrap(),
    )
}

/// Returns the ISO 8601 representation of a month ("2023-01")
pub fn month_to_part(date: &time::Date) -> String {
    format!("{}-{:02}", date.year(), date.month() as u8)
}

fn pk_to_blob_name(prefix: &str, icao: &str, date: &time::Date) -> String {
    format!(
        "{prefix}icao_number={icao}/month={}/data.json",
        month_to_part(date)
    )
}

/// Returns the first day of the next month
fn first_of_next_month(month: &time::Date) -> time::Date {
    let next_month = month.month().next();
    (next_month == time::Month::January)
        .then(|| {
            time::Date::from_calendar_date(month.year() + 1, time::Month::January, 1)
                .expect("day 1 never errors")
        })
        .unwrap_or_else(|| {
            time::Date::from_calendar_date(month.year(), next_month, 1).expect("day 1 never errors")
        })
}

/// Returns the positions of an aircraft at a given month, ordered by timestamp
/// # Implementation
/// This function is idempotent but not pure:
/// * the data is retrieved from `https://globe.adsbexchange.com`
/// * the call is cached on local disk or Remote Blob (depending on `client` configuration)
pub async fn month_positions(
    icao_number: &str,
    month: time::Date,
    client: &dyn BlobStorageProvider,
) -> Result<Vec<Position>, std::io::Error> {
    log::info!("month_positions({icao_number},{month})");
    assert_eq!(month.day(), 1);
    let blob_name = pk_to_blob_name(DATABASE, &icao_number, &month);

    let to = first_of_next_month(&month);
    let action = fs::CacheAction::from_date(&to);

    let fetch = async {
        // fetch all positions for the month for icao
        let tasks = cached_aircraft_positions(icao_number, month, to, client);
        let mut positions: Vec<Position> = futures::stream::iter(tasks)
            // limit concurrent tasks
            .buffered(5)
            .try_collect::<Vec<_>>()
            .await
            .map(|x| x.into_iter().flatten().collect())?;

        // sort them
        positions.sort_unstable_by_key(|p| p.datetime());
        let mut bytes: Vec<u8> = Vec::new();
        serde_json::to_writer(&mut bytes, &positions)?;
        Ok(bytes)
    };

    let r = fs::cached_call(&blob_name, fetch, client, action).await?;
    Ok(serde_json::from_slice(&r)?)
}

/// Returns a list of positions within two dates ordered by timestamp
/// # Implementation
/// This function is idempotent but not pure:
/// * the data is retrieved from `https://globe.adsbexchange.com`
/// * the call is cached on local disk or Remote Blob (depending on `client` configuration)
/// * the data is retrieved in batches of months and cached, to reduce IO
pub async fn aircraft_positions(
    from: Date,
    to: Date,
    icao_number: &str,
    client: &dyn BlobStorageProvider,
) -> Result<Vec<Position>, Box<dyn Error>> {
    let dates = super::DateIter {
        from,
        to,
        increment: time::Duration::days(1),
    };

    let months = dates
        .clone()
        .map(|x| {
            time::Date::from_calendar_date(x.year(), x.month(), 1).expect("day 1 never errors")
        })
        .collect::<HashSet<_>>();

    let tasks = months
        .into_iter()
        .map(|month| async move { month_positions(icao_number, month, client).await });

    let positions = futures::stream::iter(tasks)
        // limit concurrent tasks
        .buffered(10)
        .try_collect::<Vec<_>>()
        .await?;

    let mut positions = positions
        .into_iter()
        .flatten()
        .filter(|p| (p.datetime().date() >= from) && (p.datetime().date() < to))
        .collect::<Vec<_>>();
    positions.sort_unstable_by_key(|p| p.datetime());
    Ok(positions)
}

/// Returns the set of (icao number, month) that exist in the container prefixed by `dataset`
pub async fn list(
    prefix: &str,
    client: &dyn BlobStorageProvider,
) -> Result<HashSet<(Arc<str>, time::Date)>, std::io::Error> {
    Ok(client
        .list(prefix)
        .await?
        .into_iter()
        .map(|blob| blob_name_to_pk(prefix, &blob))
        .collect())
}

/// Returns the positions of an aircraft at a given month from the database.
/// Use [`list_months_positions`] to list which exist.
pub async fn get_month_positions(
    icao_number: &str,
    month: time::Date,
    client: &dyn BlobStorageProvider,
) -> Result<Vec<Position>, std::io::Error> {
    log::info!("get_months_positions({icao_number},{month})");
    assert_eq!(month.day(), 1);
    let blob_name = pk_to_blob_name(DATABASE, &icao_number, &month);

    let r = client
        .maybe_get(&blob_name)
        .await?
        .ok_or_else(|| std::io::Error::other(format!("{blob_name} does not exist")))?;
    Ok(serde_json::from_slice(&r)?)
}

/// Returns the set of (icao, month) that exists in the db
pub async fn list_months_positions(
    client: &dyn BlobStorageProvider,
) -> Result<HashSet<(Arc<str>, time::Date)>, std::io::Error> {
    list(DATABASE, client).await
}

#[cfg(test)]
mod test {
    use time::macros::date;

    use super::*;

    #[test]
    fn roundtrip() {
        let icao: Arc<str> = "aa".into();
        let month = date!(2022 - 02 - 01);
        assert_eq!(
            blob_name_to_pk(DATABASE, &pk_to_blob_name(DATABASE, icao.as_ref(), &month)),
            (icao, month)
        )
    }

    #[test]
    fn _first_of_next_month() {
        assert_eq!(
            first_of_next_month(&date!(2022 - 02 - 01)),
            date!(2022 - 03 - 01)
        );
        assert_eq!(
            first_of_next_month(&date!(2023 - 12 - 01)),
            date!(2024 - 01 - 01)
        );
    }
}
