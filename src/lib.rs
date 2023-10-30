mod aircrafts;
mod airports;
mod csv;
mod emissions;
mod emissions_private_jet;
mod icao_to_trace;
mod legs;
mod model;
mod number_to_icao;
mod owners;

pub use aircrafts::*;
pub use airports::*;
pub use emissions::*;
pub use emissions_private_jet::*;
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn dist() {
        let ekbi = (55.739834, 9.138823);
        let lira = (41.849808, 12.559995);
        let d = distance(ekbi, lira);
        assert_eq!(d, 1564.467807);
    }
}
