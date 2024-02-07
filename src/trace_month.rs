use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::Arc,
};

use futures::{StreamExt, TryStreamExt};
use time::Date;

use super::Position;
use crate::{cached_aircraft_positions, fs, fs_s3};

static DATABASE: &'static str = "position";

fn blob_name_to_pk(blob: &str) -> (Arc<str>, time::Date) {
    let bla = &blob["trace/icao_number=".len()..];
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

fn pk_to_blob_name(icao: &str, date: &time::Date) -> String {
    format!(
        "{DATABASE}/icao_number={icao}/month={}-{:02}/data.json",
        date.year(),
        date.month() as u8
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

pub async fn month_positions(
    month: time::Date,
    icao_number: &str,
    client: Option<&fs_s3::ContainerClient>,
) -> Result<Vec<Position>, Box<dyn Error>> {
    log::info!("month_positions({month},{icao_number})");
    assert_eq!(month.day(), 1);
    let blob_name = pk_to_blob_name(&icao_number, &month);

    let (from, to) = get_month(&month);
    let action = fs::CacheAction::from_date(&to);

    // returns positions in the month, cached
    let fetch = async {
        let positions = cached_aircraft_positions(from, to, icao_number, client).await?;

        let positions = positions.into_iter().map(|(_, p)| p).collect::<Vec<_>>();

        let mut bytes: Vec<u8> = Vec::new();
        serde_json::to_writer(&mut bytes, &positions)?;
        Ok(bytes)
    };

    let r = fs_s3::cached_call(&blob_name, fetch, action, client).await?;
    Ok(serde_json::from_slice(&r)?)
}

/// Returns a list of positions within two dates
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

    let mut positions = positions.into_iter().flatten().collect::<Vec<_>>();
    positions.sort_unstable_by_key(|p| p.datetime());
    Ok(positions)
}

/// Returns the set of (icao, month) that exists in the db
pub async fn existing_months_positions(
    client: &fs_s3::ContainerClient,
) -> Result<HashSet<(Arc<str>, time::Date)>, fs_s3::Error> {
    Ok(client
        .client
        .list_objects_v2()
        .bucket(&client.bucket)
        .prefix(format!("{DATABASE}/"))
        .into_paginator()
        .send()
        .try_collect()
        .await
        .map_err(|e| fs_s3::Error::from(e.to_string()))?
        .into_iter()
        .map(|response| {
            response
                .contents()
                .iter()
                .filter_map(|blob| blob.key())
                .map(|blob| blob_name_to_pk(&blob))
                .collect::<Vec<_>>()
        })
        .flatten()
        .collect())
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
            blob_name_to_pk(&pk_to_blob_name(icao.as_ref(), &month)),
            (icao, month)
        )
    }
}
