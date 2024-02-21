use std::{collections::HashSet, error::Error, sync::Arc};

use futures::{StreamExt, TryStreamExt};
use time::Date;

use super::Position;
use crate::{cached_aircraft_positions, fs, fs_s3, BlobStorageProvider};

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

fn get_month(current: &time::Date) -> (time::Date, time::Date) {
    let first_of_month =
        time::Date::from_calendar_date(current.year(), current.month(), 1).unwrap();

    let next_month = current.month().next();
    let first_of_next_month = (next_month == time::Month::January)
        .then(|| {
            time::Date::from_calendar_date(current.year() + 1, time::Month::January, 1)
                .expect("day 1 never errors")
        })
        .unwrap_or_else(|| {
            time::Date::from_calendar_date(current.year(), next_month, 1)
                .expect("day 1 never errors")
        });
    (first_of_month, first_of_next_month)
}

/// Returns a list of positions of a date ordered by timestamp
/// # Implementation
/// This function is idempotent but not pure:
/// * the data is retrieved from `https://globe.adsbexchange.com`
/// * the call is cached on local disk or Remote Blob (depending on `client` configuration)
pub async fn month_positions(
    month: time::Date,
    icao_number: &str,
    client: Option<&fs_s3::ContainerClient>,
) -> Result<Vec<Position>, std::io::Error> {
    log::info!("month_positions({month},{icao_number})");
    assert_eq!(month.day(), 1);
    let blob_name = pk_to_blob_name(DATABASE, &icao_number, &month);

    let (from, to) = get_month(&month);
    let action = fs::CacheAction::from_date(&to);

    // returns positions in the month, cached
    let fetch = async {
        let mut positions = cached_aircraft_positions(from, to, icao_number, client).await?;
        positions.sort_unstable_by_key(|p| p.datetime());
        let mut bytes: Vec<u8> = Vec::new();
        serde_json::to_writer(&mut bytes, &positions).map_err(std::io::Error::other)?;
        Ok(bytes)
    };

    let r = fs_s3::cached_call(&blob_name, fetch, action, client).await?;
    serde_json::from_slice(&r).map_err(std::io::Error::other)
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
    client: Option<&fs_s3::ContainerClient>,
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
        .map(|month| async move { month_positions(month, icao_number, client).await });

    let positions = futures::stream::iter(tasks)
        // limit concurrent tasks
        .buffered(1)
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
pub async fn existing<B: BlobStorageProvider>(
    prefix: &str,
    client: &B,
) -> Result<HashSet<(Arc<str>, time::Date)>, B::Error> {
    Ok(client
        .list(prefix)
        .await?
        .into_iter()
        .map(|blob| blob_name_to_pk(prefix, &blob))
        .collect())
}

/// Returns the set of (icao, month) that exists in the db
pub async fn existing_months_positions<B: BlobStorageProvider>(
    client: &B,
) -> Result<HashSet<(Arc<str>, time::Date)>, B::Error> {
    existing(DATABASE, client).await
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
}
