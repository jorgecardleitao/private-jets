mod aircraft_db;
mod aircraft_owners;
mod aircraft_types;
mod airports;
mod csv;
mod emissions;
pub(crate) mod fs;
pub mod fs_azure;
mod icao_to_trace;
mod legs;
mod model;
mod owners;
mod trace_month;

use std::sync::Arc;

pub use aircraft_db::*;
pub use aircraft_owners::*;
pub use aircraft_types::*;
pub use airports::*;
pub use emissions::*;
pub use fs::BlobStorageProvider;
pub use icao_to_trace::*;
pub use legs::*;
pub use model::*;
pub use owners::*;

/// A position of an aircraft
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Position {
    /// Aircraft transponder declares the aircraft is grounded
    Grounded {
        icao: Arc<str>,
        datetime: time::PrimitiveDateTime,
        latitude: f64,
        longitude: f64,
    },
    /// Aircraft transponder declares the aircraft is flying at a given altitude
    Flying {
        icao: Arc<str>,
        datetime: time::PrimitiveDateTime,
        latitude: f64,
        longitude: f64,
        altitude: f64,
    },
}

impl Position {
    pub fn icao(&self) -> &Arc<str> {
        match self {
            Position::Flying { icao, .. } | Position::Grounded { icao, .. } => icao,
        }
    }

    pub fn latitude(&self) -> f64 {
        match *self {
            Position::Flying { latitude, .. } | Position::Grounded { latitude, .. } => latitude,
        }
    }

    pub fn longitude(&self) -> f64 {
        match *self {
            Position::Flying { longitude, .. } | Position::Grounded { longitude, .. } => longitude,
        }
    }

    pub fn pos(&self) -> (f64, f64) {
        (self.latitude(), self.longitude())
    }

    pub fn altitude(&self) -> f64 {
        match *self {
            Position::Flying { altitude, .. } => altitude,
            Position::Grounded { .. } => 0.0,
        }
    }

    pub fn datetime(&self) -> time::PrimitiveDateTime {
        match *self {
            Position::Flying { datetime, .. } => datetime,
            Position::Grounded { datetime, .. } => datetime,
        }
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
    from.distance_to(&to).unwrap().meters() / 1000.0
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

    #[test]
    fn work() {
        assert_eq!(
            DateIter {
                from: time::Date::from_calendar_date(2022, time::Month::January, 1).unwrap(),
                to: time::Date::from_calendar_date(2022, time::Month::January, 3).unwrap(),
                increment: time::Duration::days(1)
            }
            .collect::<Vec<_>>(),
            vec![
                time::Date::from_calendar_date(2022, time::Month::January, 1).unwrap(),
                time::Date::from_calendar_date(2022, time::Month::January, 2).unwrap()
            ]
        );
    }
}
