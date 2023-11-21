mod aircraft_db;
mod aircraft_owners;
mod aircraft_types;
mod airports;
mod csv;
mod emissions;
pub mod fs_azure;
mod icao_to_trace;
mod legs;
mod model;
mod owners;

pub use aircraft_db::*;
pub use aircraft_owners::*;
pub use aircraft_types::*;
pub use airports::*;
pub use emissions::*;
pub use icao_to_trace::*;
pub use legs::*;
pub use model::*;
pub use owners::*;

/// A position of an aircraft
#[derive(Debug, Clone, Copy)]
pub enum Position {
    /// Aircraft transponder declares the aircraft is grounded
    Grounded {
        datetime: time::PrimitiveDateTime,
        latitude: f64,
        longitude: f64,
    },
    /// Aircraft transponder declares the aircraft is flying at a given altitude
    Flying {
        datetime: time::PrimitiveDateTime,
        latitude: f64,
        longitude: f64,
        altitude: f64,
    },
}

impl Position {
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
        let maybe_next = self.from.saturating_add(self.increment);
        self.from = maybe_next;
        (maybe_next < self.to).then_some(maybe_next)
    }
}
