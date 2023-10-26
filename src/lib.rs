mod aircrafts;
mod airports;
mod emissions;
mod icao_to_trace;
mod legs;
mod model;
mod number_to_icao;
mod owners;

pub use aircrafts::*;
pub use airports::*;
pub use emissions::*;
pub use icao_to_trace::*;
pub use legs::*;
pub use model::*;
pub use number_to_icao::*;
pub use owners::*;

/// Returns the distance between two geo-points in km
fn distance(from: (f64, f64), to: (f64, f64)) -> f64 {
    let from = geoutils::Location::new(from.0, from.1);
    let to = geoutils::Location::new(to.0, to.1);
    from.distance_to(&to).unwrap().meters() / 1000.0
}
