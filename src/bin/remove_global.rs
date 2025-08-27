use std::error::Error;

use fs::BlobStorageProvider;
use futures::StreamExt;
use simple_logger::SimpleLogger;

const ABOUT: &'static str = r#"Removes the database"#;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let access_key = std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not set");
    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .expect("AWS_SECRET_ACCESS_KEY not set");

    let client = fs::s3::client(access_key, secret_access_key).await;
    let client = &client;

    let days = flights::DateIter {
        from: time::Date::from_calendar_date(2019, time::Month::try_from(1).unwrap(), 1).unwrap(),
        to: time::Date::from_calendar_date(2025, time::Month::try_from(12).unwrap(), 31).unwrap(),
        increment: time::Duration::days(1),
    };

    for date in days {
        let format = time::format_description::parse("[year]-[month]-[day]").unwrap();
        let date = date.format(&format).unwrap();
        log::info!("Removing {date}");
        let blobs = client.list(&format!("globe_history/{date}/")).await?;
        log::info!("Removing {}", blobs.len());

        let tasks = blobs
            .into_iter()
            .map(|b| async move { client.delete(&b).await })
            .collect::<Vec<_>>();

        futures::stream::iter(tasks)
            // limit concurrent tasks
            .buffered(200)
            // continue if error
            .map(|r| {
                if let Err(e) = r {
                    log::error!("{e}");
                }
            })
            .collect::<Vec<_>>()
            .await;
    }
    Ok(())
}
