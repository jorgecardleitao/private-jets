use rand::Rng;
use reqwest::header;
use reqwest::{self, StatusCode};

fn last_2(icao: &str) -> &str {
    let bytes = icao.as_bytes();
    std::str::from_utf8(&bytes[bytes.len() - 2..]).unwrap()
}

fn to_url(icao: &str, date: &str) -> String {
    let date = date.replace("-", "/");
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

fn globe_history(icao: &str, date: &str) -> Result<String, Box<dyn std::error::Error>> {
    let referer = format!("https://globe.adsbexchange.com/?icao={icao}&lat=54.448&lon=10.602&zoom=7.0&showTrace={date}");
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

    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let response = client.get(url).headers(headers).send()?;
    if response.status() == StatusCode::OK {
        Ok(response.text()?)
    } else if response.status() == StatusCode::NOT_FOUND {
        Ok(format!(
            r#"{{
            "icao": "{icao}",
            "noRegData": true,
            "timestamp": 1697155200.000,
            "trace": []
        }}"#
        ))
    } else {
        Err("could not retrieve data from globe.adsbexchange.com".into())
    }
}

fn globe_history_cached(icao: &str, date: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let file_path = format!("database/{icao}_{date}.json");
    if !std::path::Path::new(&file_path).exists() {
        let data = globe_history(&icao, date)?;
        std::fs::write(&file_path, data)?;
    }

    Ok(std::fs::read(file_path)?)
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
pub fn trace_cached(
    icao: &str,
    date: &str,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let data = globe_history_cached(icao, date)?;

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
