#[forbid(unsafe_code)]
pub mod aircraft;
mod aircraft_models;
mod aircraft_owners;
mod airports;
mod country;
pub mod csv;
mod emissions;
pub(crate) mod fs;
pub mod fs_s3;
mod icao_to_trace;
pub mod io;
mod legs;
mod model;
mod owners;
mod private_emissions;
mod private_jets_in_time;
pub mod serde;
mod trace_month;

pub use aircraft_models::*;
pub use aircraft_owners::*;
pub use airports::*;
pub(crate) use country::CountryIcaoRanges;
pub use emissions::*;
pub use fs::{BlobStorageProvider, LocalDisk};
pub use icao_to_trace::*;
pub use legs::*;
pub use model::*;
pub use owners::*;
pub use private_emissions::*;
pub use private_jets_in_time::private_jets_in_month;

/// A position of an aircraft
#[derive(Debug, Clone, PartialEq, ::serde::Serialize, ::serde::Deserialize)]
pub struct Position {
    #[serde(with = "time::serde::rfc3339")]
    datetime: time::OffsetDateTime,
    latitude: f64,
    longitude: f64,
    /// None means on the ground
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    altitude: Option<f64>,
}

impl Position {
    pub fn flying(&self) -> bool {
        self.altitude.is_some()
    }

    pub fn grounded(&self) -> bool {
        self.altitude.is_none()
    }

    pub fn latitude(&self) -> f64 {
        self.latitude
    }

    pub fn longitude(&self) -> f64 {
        self.longitude
    }

    pub fn pos(&self) -> (f64, f64) {
        (self.latitude(), self.longitude())
    }

    pub fn altitude(&self) -> f64 {
        self.altitude.unwrap_or(0.0)
    }

    pub fn datetime(&self) -> time::OffsetDateTime {
        self.datetime
    }

    /// Returns the distance to another [`Position`] in km
    pub fn distace(&self, other: &Self) -> f64 {
        distance(self.pos(), other.pos())
    }
}

/// Returns the distance between two geo-points in km
fn distance(from: (f64, f64), to: (f64, f64)) -> f64 {
    let from = geoutils::Location::new(from.0, from.1);
    let to = geoutils::Location::new(to.0, to.1);
    from.haversine_distance_to(&to).meters() / 1000.0
}

/// An iterator between two [`time::Date`]s in increments
/// The result is exclusive, i.e. the iterator has two items when increment is one day
/// from 2022-01-01 and 2022-01-03
#[derive(Clone, Copy)]
pub struct DateIter {
    pub from: time::Date,
    pub to: time::Date,
    pub increment: time::Duration,
}

impl Iterator for DateIter {
    type Item = time::Date;

    fn next(&mut self) -> Option<Self::Item> {
        if self.from >= self.to {
            return None;
        }
        let maybe_next = self.from;
        self.from = self.from.saturating_add(self.increment);
        Some(maybe_next)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use time::macros::date;

    #[test]
    fn work() {
        assert_eq!(
            DateIter {
                from: date!(2022 - 01 - 01),
                to: date!(2022 - 01 - 03),
                increment: time::Duration::days(1)
            }
            .collect::<Vec<_>>(),
            vec![date!(2022 - 01 - 01), date!(2022 - 01 - 02)]
        );
    }
}
