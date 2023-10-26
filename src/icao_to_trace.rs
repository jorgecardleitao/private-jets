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

fn trace(icao: &str, date: &str, cookie: &str) -> Result<String, Box<dyn std::error::Error>> {
    let icao = icao.to_ascii_lowercase();

    let referer = format!("https://globe.adsbexchange.com/?icao={icao}&lat=54.448&lon=10.602&zoom=7.0&showTrace={date}");
    let url = to_url(&icao, date);

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
    headers.insert(header::COOKIE, cookie.parse().unwrap());
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

pub fn trace_cached(
    icao: &str,
    date: &str,
    cookie: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let file_path = format!("database/{icao}_{date}.json");
    if !std::path::Path::new(&file_path).exists() {
        let data = trace(icao, date, cookie)?;
        std::fs::write(&file_path, data)?;
    }

    let data = std::fs::read(file_path)?;
    Ok(serde_json::from_slice(&data)?)
}
