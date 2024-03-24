use rand::Rng;
use reqwest::header;
use reqwest::{self, StatusCode};
use reqwest_middleware::ClientBuilder;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use time::Date;
use time::OffsetDateTime;

use super::Position;
use crate::{fs, BlobStorageProvider};

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

pub(crate) static DATABASE: &'static str = "globe_history";

fn cache_file_path(icao: &str, date: &time::Date) -> String {
    format!("{DATABASE}/{date}/trace_full_{icao}.json")
}

async fn globe_history(icao: &str, date: &time::Date) -> Result<Vec<u8>, std::io::Error> {
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

    // Retry up to 5 times with increasing intervals between attempts.
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(5);
    let client = ClientBuilder::new(reqwest::Client::new())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    let response = client
        .get(url)
        .headers(headers)
        .send()
        .await
        .map_err(std::io::Error::other)?;
    if response.status() == StatusCode::OK {
        Ok(response
            .bytes()
            .await
            .map_err(std::io::Error::other)?
            .to_vec())
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
        response
            .text()
            .await
            .map_err(std::io::Error::other)
            .map(|x| x.into())
    }
}

/// Returns a map between tail number (e.g. "OYTWM": "45D2ED")
/// Caches the first time it is executed
/// Caching is skipped if `date` is either today (UTC) or in the future
/// as the global history is only available at the end of the day
async fn globe_history_cached(
    icao: &str,
    date: &time::Date,
    client: &dyn BlobStorageProvider,
) -> Result<Vec<u8>, std::io::Error> {
    let blob_name = cache_file_path(icao, date);
    let action = fs::CacheAction::from_date(&date);
    let fetch = globe_history(&icao, date);

    Ok(fs::cached_call(&blob_name, fetch, client, action).await?)
}

fn compute_trace(data: &[u8]) -> Result<(f64, Vec<serde_json::Value>), std::io::Error> {
    if data.len() == 0 {
        return Ok((0.0, vec![]));
    };
    let mut value = serde_json::from_slice::<serde_json::Value>(&data)?;
    let Some(obj) = value.as_object_mut() else {
        return Ok((0.0, vec![]));
    };
    let Some(timestamp) = obj.get("timestamp") else {
        return Ok((0.0, vec![]));
    };
    let timestamp = timestamp.as_f64().unwrap();
    let Some(obj) = obj.get_mut("trace") else {
        return Ok((0.0, vec![]));
    };
    let Some(trace) = obj.as_array_mut() else {
        return Ok((0.0, vec![]));
    };

    Ok((timestamp, std::mem::take(trace)))
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
async fn trace_cached(
    icao: &str,
    date: &time::Date,
    client: &dyn BlobStorageProvider,
) -> Result<(f64, Vec<serde_json::Value>), std::io::Error> {
    compute_trace(&globe_history_cached(icao, date, client).await?)
}

fn compute_positions(start_trace: (f64, Vec<serde_json::Value>)) -> impl Iterator<Item = Position> {
    use time::ext::NumericalDuration;

    let (start, trace) = start_trace;
    let start = OffsetDateTime::from_unix_timestamp(start as i64).unwrap();

    trace.into_iter().filter_map(move |entry| {
        let delta = entry[0].as_f64().unwrap().seconds();
        let datetime = start + delta;
        let latitude = entry[1].as_f64().unwrap();
        let longitude = entry[2].as_f64().unwrap();
        entry[3]
            .as_str()
            .and_then(|x| {
                (x == "ground").then_some(Position {
                    datetime,
                    latitude,
                    longitude,
                    altitude: None,
                })
            })
            .or_else(|| {
                entry[3].as_f64().and_then(|altitude| {
                    Some(Position {
                        datetime,
                        latitude,
                        longitude,
                        altitude: Some(altitude),
                    })
                })
            })
    })
}

/// Returns an iterator of [`Position`] over the trace of `icao` on day `date` according
/// to the [methodology `M-3`](../methodology.md).
pub async fn positions(
    icao_number: &str,
    date: time::Date,
    client: &dyn BlobStorageProvider,
) -> Result<impl Iterator<Item = Position>, std::io::Error> {
    trace_cached(icao_number, &date, client)
        .await
        .map(compute_positions)
}

pub(crate) fn cached_aircraft_positions<'a>(
    icao_number: &'a str,
    from: Date,
    to: Date,
    client: &'a dyn BlobStorageProvider,
) -> impl Iterator<
    Item = impl futures::future::Future<Output = Result<Vec<Position>, std::io::Error>> + 'a,
> + 'a {
    super::DateIter {
        from,
        to,
        increment: time::Duration::days(1),
    }
    .map(move |date| async move {
        Result::<_, std::io::Error>::Ok(
            positions(icao_number, date, client)
                .await?
                .collect::<Vec<_>>(),
        )
    })
}

pub use crate::trace_month::*;

#[cfg(test)]
mod test {
    use time::macros::date;

    use super::*;

    /// Compare against https://globe.adsbexchange.com/?icao=45860d&showTrace=2019-01-04&leg=1
    #[tokio::test]
    async fn work() {
        let data = globe_history("45860d", &date!(2019 - 01 - 04))
            .await
            .unwrap();
        let first = compute_positions(compute_trace(&data).unwrap())
            .next()
            .unwrap();
        assert_eq!(first.datetime.hour(), 6);
        assert_eq!(first.datetime.minute(), 54);
        assert_eq!(first.grounded(), true);
    }

    #[tokio::test]
    async fn edge_cases() {
        assert_eq!(compute_trace(b"").unwrap().1.len(), 0);
        assert_eq!(compute_trace(b"[]").unwrap().1.len(), 0);
        assert_eq!(compute_trace(b"{}").unwrap().1.len(), 0);
        assert_eq!(compute_trace(b"{\"timestamp\": 1.0}").unwrap().1.len(), 0);
        assert_eq!(
            compute_trace(b"{\"timestamp\": 1.0, \"trace\": {}}")
                .unwrap()
                .1
                .len(),
            0
        );
    }
}
