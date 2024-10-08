use std::error::Error;

use flights::{fs::BlobStorageProvider, fs::LocalDisk, legs::Leg};
use time::{
    macros::{date, datetime},
    Date,
};

/// Verifies that we compute the same number of legs and their duration
/// as in source
/// https://globe.adsbexchange.com/?icao=45d2ed&lat=54.128&lon=9.185&zoom=5.0&showTrace=2023-10-13
#[tokio::test]
async fn acceptance_legs() -> Result<(), Box<dyn Error>> {
    let positions =
        flights::icao_to_trace::positions("45d2ed", date!(2023 - 10 - 13), &LocalDisk).await?;
    let legs = flights::legs::legs(positions).collect::<Vec<_>>();

    assert_eq!(legs.len(), 2);

    let expected = datetime!(2023 - 10 - 13 15:24:49) - datetime!(2023 - 10 - 13 13:21:59);
    let diff = (legs[0].duration().as_seconds_f32() - expected.as_seconds_f32()).abs();
    assert!(diff < 300.0);

    let expected = datetime!(2023 - 10 - 13 17:34:02) - datetime!(2023 - 10 - 13 15:58:33);
    let diff = (legs[1].duration().as_seconds_f32() - expected.as_seconds_f32()).abs();
    assert!(diff < 300.0);

    Ok(())
}

#[tokio::test]
async fn legs_() -> Result<(), Box<dyn Error>> {
    let positions =
        flights::icao_to_trace::positions("459cd3", date!(2023 - 11 - 17), &LocalDisk).await?;
    let legs = flights::legs::legs(positions);

    // same as ads-b computes: https://globe.adsbexchange.com/?icao=459cd3&lat=53.265&lon=8.038&zoom=6.5&showTrace=2023-11-17
    assert_eq!(legs.count(), 5);
    Ok(())
}

async fn legs(
    from: Date,
    to: Date,
    icao_number: &str,
    client: &dyn BlobStorageProvider,
) -> Result<Vec<Leg>, Box<dyn Error>> {
    let positions =
        flights::icao_to_trace::aircraft_positions(from, to, icao_number, client).await?;
    Ok(flights::legs::legs(positions.into_iter()).collect::<Vec<_>>())
}

/// Verifies that condition 2. of `M-identify-legs` is correctly applied.
/// https://globe.adsbexchange.com/?icao=458d90&lat=53.265&lon=8.038&zoom=6.5&showTrace=2023-07-21
#[tokio::test]
async fn ads_b_lost_on_ground() -> Result<(), Box<dyn Error>> {
    let legs = legs(
        date!(2023 - 07 - 21),
        date!(2023 - 07 - 23),
        "458d90",
        &LocalDisk,
    )
    .await?;
    assert_eq!(legs.len(), 6);
    Ok(())
}

/// Verifies that condition 2. of `M-identify-legs` is correctly applied.
/// https://globe.adsbexchange.com/?icao=459257&showTrace=2023-12-17
#[tokio::test]
async fn case_459257_2023_12_17() -> Result<(), Box<dyn Error>> {
    let legs = legs(
        date!(2023 - 12 - 17),
        date!(2023 - 12 - 20),
        "459257",
        &LocalDisk,
    )
    .await?;
    assert_eq!(legs.len(), 4);
    Ok(())
}

/// Verifies that condition 3. of `M-identify-legs` is correctly applied.
/// Case of losing signal for 2 days mid flight while traveling to central Africa.
/// https://globe.adsbexchange.com/?icao=45dd84&lat=9.613&lon=22.035&zoom=3.8&showTrace=2023-12-08
#[tokio::test]
async fn case_45dd84_2023_12_06() -> Result<(), Box<dyn Error>> {
    let legs = legs(
        date!(2023 - 12 - 06),
        date!(2023 - 12 - 09),
        "45dd84",
        &LocalDisk,
    )
    .await?;
    assert_eq!(legs.len(), 3);
    let day = 24.0 * 60.0 * 60.0;
    assert!(legs[0].duration().as_seconds_f32() < day);
    assert!(legs[1].duration().as_seconds_f32() < day);
    assert!(legs[2].duration().as_seconds_f32() < day);
    Ok(())
}

#[tokio::test]
async fn case_45c824_2023_12_12() -> Result<(), Box<dyn Error>> {
    let legs = legs(
        date!(2023 - 12 - 12),
        date!(2023 - 12 - 16),
        "45c824",
        &LocalDisk,
    )
    .await?;

    assert_eq!(legs.len(), 3);
    let day = 24.0 * 60.0 * 60.0;
    assert!(legs[0].duration().as_seconds_f32() < day);
    assert!(legs[1].duration().as_seconds_f32() < day);
    assert!(legs[2].duration().as_seconds_f32() < day);
    Ok(())
}

#[tokio::test]
async fn gets_db_positions() -> Result<(), Box<dyn Error>> {
    let client = flights::fs_s3::anonymous_client().await;

    let _ = flights::icao_to_trace::positions("459cd3", date!(2020 - 01 - 01), &client).await?;
    Ok(())
}

#[tokio::test]
async fn gets_db_month() -> Result<(), Box<dyn Error>> {
    let client = flights::fs_s3::anonymous_client().await;

    let _ = flights::icao_to_trace::get_month_positions("459cd3", date!(2020 - 01 - 01), &client)
        .await?;
    Ok(())
}

#[tokio::test]
async fn private_jets_in_month() -> Result<(), Box<dyn Error>> {
    let client = flights::fs_s3::anonymous_client().await;

    let aircraft = flights::private_jets_in_month(2022..2024, None, &client).await?;

    // this number should be constant, as the db of aircrafts does not change in the past
    assert_eq!(aircraft.len(), 29425 * 24);
    Ok(())
}
