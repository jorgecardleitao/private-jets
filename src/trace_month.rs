use super::icao_to_trace::*;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::Arc,
};

use azure_storage_blobs::container::operations::BlobItem;
use futures::{StreamExt, TryStreamExt};
use time::Date;

use super::Position;
use crate::{fs, fs_azure};

fn cache_file_path(icao: &str, date: &time::Date) -> String {
    format!(
        "{DIRECTORY}/{DATABASE}/{}-{:02}/trace_full_{icao}.json",
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
    client: Option<&super::fs_azure::ContainerClient>,
) -> Result<HashMap<Date, Vec<Position>>, Box<dyn Error>> {
    log::info!("month_positions({month},{icao_number})");
    assert_eq!(month.day(), 1);
    let blob_name = cache_file_path(&icao_number, &month);

    let (from, to) = get_month(&month);
    let action = fs::CacheAction::from_date(&to);

    // returns positions in the month, cached
    let fetch = async {
        let positions = cached_aircraft_positions(from, to, icao_number, client).await?;

        let positions = positions
            .into_iter()
            .map(|(d, p)| (d.to_string(), p))
            .collect::<HashMap<_, _>>();

        let mut bytes: Vec<u8> = Vec::new();
        serde_json::to_writer(&mut bytes, &positions)?;
        Ok(bytes)
    };

    let r = fs_azure::cached_call(&blob_name, fetch, action, client).await?;
    Ok(serde_json::from_slice(&r)?)
}

/// Returns a map (date -> positions) for a given icao number.
/// # Implementation
/// This function is idempotent but not pure:
/// * the data is retrieved from `https://globe.adsbexchange.com`
/// * the call is cached on local disk or Azure Blob (depending on `client` configuration)
/// * the data is retrieved in batches of months and cached, to reduce IO
pub async fn aircraft_positions(
    from: Date,
    to: Date,
    icao_number: &str,
    client: Option<&super::fs_azure::ContainerClient>,
) -> Result<HashMap<Date, Vec<Position>>, Box<dyn Error>> {
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

    // flatten positions so we can look days on them
    let mut positions = positions.into_iter().flatten().collect::<HashMap<_, _>>();

    Ok(dates
        .map(|date| {
            (
                date,
                // we can .remove because dates are guaranteed to be unique (and avoids clone)
                positions
                    .remove(&date)
                    .expect("That every date is covered on months; every date is unique"),
            )
        })
        .collect())
}

async fn existing_blobs(
    months: &[time::Date],
    client: &fs_azure::ContainerClient,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let tasks = months
        .iter()
        .map(|month| {
            client
                .client
                .list_blobs()
                .prefix(format!(
                    "database/globe_history/{}-{:02}",
                    month.year(),
                    month.month() as u8
                ))
                .into_stream()
                .map(|response| {
                    let blobs = response?.blobs;
                    Ok::<_, azure_core::Error>(
                        blobs
                            .items
                            .into_iter()
                            .filter_map(|blob| match blob {
                                BlobItem::Blob(blob) => Some(blob.name),
                                BlobItem::BlobPrefix(_) => None,
                            })
                            .collect::<Vec<_>>(),
                    )
                })
                .try_collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let r = futures::stream::iter(tasks)
        .buffered(100)
        .try_collect::<Vec<_>>()
        .await?;

    Ok(r.into_iter()
        .flatten()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>())
}

fn to_set(blobs: Vec<String>) -> HashSet<(Arc<str>, time::Date)> {
    blobs
        .into_iter()
        .filter_map(|blob| {
            let bla = &blob["database/globe_history/".len()..];
            if &bla[7..8] != "/" {
                // complete date - ignore
                return None;
            }
            let date = &bla[.."2024-01".len()];
            let end = bla.len();
            let icao = &bla["2024-01/trace_full_".len()..end - ".json".len()];
            Some((
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
            ))
        })
        .collect::<HashSet<_>>()
}

/// Returns the set of (icao, month) that exists in the db
pub async fn existing_months_positions(
    months: &[time::Date],
    client: &fs_azure::ContainerClient,
) -> Result<HashSet<(Arc<str>, time::Date)>, Box<dyn std::error::Error>> {
    let blobs = existing_blobs(&months, &client).await?;
    Ok(to_set(blobs))
}
