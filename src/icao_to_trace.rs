use std::error::Error;

use rand::Rng;
use reqwest::header;
use reqwest::{self, StatusCode};
use time::PrimitiveDateTime;

use crate::fs_azure;

use super::Position;

fn last_2(icao: &str) -> &str {
    let bytes = icao.as_bytes();
    std::str::from_utf8(&bytes[bytes.len() - 2..]).unwrap()
}

fn to_url(icao: &str, date: &time::Date) -> String {
    let format = time::format_description::parse("[year]/[month]/[day]").unwrap();
    let date = date.format(&format).unwrap();
    let last_2 = last_2(icao);
    format!("https://globe.adsbexchange.com/globe_history/{date}/traces/{last_2}/trace_full_{icao}.json")
}

fn adsbx_sid() -> String {
    // from https://globe.adsbexchange.com/adsbx_comb_index_tarmisc_min_fc01616f370a6163a397b31cbee9dcd9.js
    //ts+1728e5+"_"+Math.random().toString(36).substring(2,15),2)
    let time = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        + 1728 * 100000;

    let random_chars = std::iter::repeat(())
        .map(|_| rand::thread_rng().sample(rand::distributions::Alphanumeric))
        .take(13)
        .map(|x| x as char)
        .collect::<String>();
    format!("{time}_{random_chars}")
}

static DIRECTORY: &'static str = "database";
static DATABASE: &'static str = "globe_history";

fn cache_file_path(icao: &str, date: &time::Date) -> String {
    format!("{DIRECTORY}/{DATABASE}/{date}/trace_full_{icao}.json")
}

async fn globe_history(
    icao: &str,
    date: &time::Date,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    log::info!("globe_history({icao},{date})");
    let referer =
        format!("https://globe.adsbexchange.com/?icao={icao}&lat=54.448&lon=10.602&zoom=7.0");
    let url = to_url(icao, date);

    let mut headers = header::HeaderMap::new();
    headers.insert(
        "User-Agent",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/118.0"
            .parse()
            .unwrap(),
    );
    headers.insert(
        "Accept",
        "application/json, text/javascript, */*; q=0.01"
            .parse()
            .unwrap(),
    );
    headers.insert("Accept-Language", "en-US,en;q=0.5".parse().unwrap());
    headers.insert("Accept-Encoding", "gzip, deflate, br".parse().unwrap());
    headers.insert("X-Requested-With", "XMLHttpRequest".parse().unwrap());
    headers.insert("Connection", "keep-alive".parse().unwrap());
    headers.insert("Referer", referer.parse().unwrap());
    headers.insert(header::COOKIE, adsbx_sid().parse().unwrap());
    headers.insert("Sec-Fetch-Dest", "empty".parse().unwrap());
    headers.insert("Sec-Fetch-Mode", "cors".parse().unwrap());
    headers.insert("Sec-Fetch-Site", "same-origin".parse().unwrap());
    headers.insert("TE", "trailers".parse().unwrap());

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let response = client.get(url).headers(headers).send().await?;
    if response.status() == StatusCode::OK {
        Ok(response.bytes().await?.to_vec())
    } else if response.status() == StatusCode::NOT_FOUND {
        Ok(format!(
            r#"{{
            "icao": "{icao}",
            "noRegData": true,
            "timestamp": 1697155200.000,
            "trace": []
        }}"#
        )
        .into_bytes())
    } else {
        Err("could not retrieve data from globe.adsbexchange.com".into())
    }
}

/// Returns  a map between tail number (e.g. "OYTWM": "45D2ED")
/// Caches to disk the first time it is executed
async fn globe_history_cached(
    icao: &str,
    date: &time::Date,
    client: Option<&fs_azure::ContainerClient>,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let blob_name = cache_file_path(icao, date);
    let fetch = globe_history(&icao, date);

    match client {
        Some(client) => crate::fs::cached(&blob_name, fetch, client).await,
        None => crate::fs::cached(&blob_name, fetch, &crate::fs::LocalDisk).await,
    }
}

/// Returns the trace of the icao number of a given day from https://adsbexchange.com.
/// * `icao` must be lowercased
/// * `date` must be a valid ISO8601 date in format `yyyy-mm-dd` and cannot be today.
///
/// The returned value is a vector where with the following by index
/// * `0` is time in seconds since midnight (f64)
/// * `1` is latitude (f64)
/// * `2` is longitude (f64)
/// * `3` is either Baro. Altitude in feet (f32) or "ground" (String)
/// # Implementation
/// Because these are historical values, this function caches them the first time it is used
/// by the two arguments
pub async fn trace_cached(
    icao: &str,
    date: &time::Date,
    client: Option<&fs_azure::ContainerClient>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let data = globe_history_cached(icao, date, client).await?;

    let mut value = serde_json::from_slice::<serde_json::Value>(&data)?;
    let trace = value
        .as_object_mut()
        .unwrap()
        .get_mut("trace")
        .unwrap()
        .as_array_mut()
        .unwrap();
    Ok(std::mem::take(trace))
}

/// Returns an iterator of [`Position`] over the trace of `icao` on day `date` assuming that
/// a flight below `threshold` feet is grounded.
pub async fn positions(
    icao: &str,
    date: time::Date,
    threshold: f64,
    client: Option<&fs_azure::ContainerClient>,
) -> Result<impl Iterator<Item = Position>, Box<dyn Error>> {
    use time::ext::NumericalDuration;
    trace_cached(icao, &date, client).await.map(move |trace| {
        trace.into_iter().map(move |entry| {
            let time_seconds = entry[0].as_f64().unwrap();
            let time = time::Time::MIDNIGHT + time_seconds.seconds();
            let datetime = PrimitiveDateTime::new(date.clone(), time);
            let latitude = entry[1].as_f64().unwrap();
            let longitude = entry[2].as_f64().unwrap();
            entry[3]
                .as_str()
                .and_then(|x| {
                    (x == "ground").then_some(Position::Grounded {
                        datetime,
                        latitude,
                        longitude,
                    })
                })
                .unwrap_or_else(|| {
                    entry[3]
                        .as_f64()
                        .and_then(|altitude| {
                            Some(if altitude < threshold {
                                Position::Grounded {
                                    datetime,
                                    latitude,
                                    longitude,
                                }
                            } else {
                                Position::Flying {
                                    datetime,
                                    latitude,
                                    longitude,
                                    altitude,
                                }
                            })
                        })
                        .unwrap_or(Position::Grounded {
                            datetime,
                            latitude,
                            longitude,
                        })
                })
        })
    })
}
