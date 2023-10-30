use std::error::Error;

use crate::trace_cached;

#[derive(Debug, Clone, Copy)]
pub enum Position {
    // time in seconds since midnight, lat, long
    Grounded(f64, f64, f64),
    // time in seconds since midnight, lat, long, height
    Flying(f64, f64, f64, f64),
}

impl Position {
    pub fn time(&self) -> f64 {
        match *self {
            Position::Flying(time, _, _, _) | Position::Grounded(time, _, _) => time,
        }
    }

    pub fn pos(&self) -> (f64, f64) {
        match *self {
            Position::Flying(_, long, lat, _) | Position::Grounded(_, long, lat) => (long, lat),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Leg {
    pub from: Position,
    pub to: Position,
}

pub fn legs(icao: &str, date: &str, cookie: &str) -> Result<Vec<Leg>, Box<dyn Error>> {
    let values = trace_cached(icao, date, cookie)?;
    let trace = values
        .as_object()
        .unwrap()
        .get("trace")
        .unwrap()
        .as_array()
        .unwrap();
    if trace.is_empty() {
        return Ok(vec![]);
    }

    let mut positions = trace.iter().map(|entry| {
        // 0 -> time
        // 1 -> latitude
        // 2 -> longitude
        // 3 -> either Baro. Altitude in feet (f32) or "ground" (str)
        let time = entry[0].as_f64().unwrap();
        let lat = entry[1].as_f64().unwrap();
        let long = entry[2].as_f64().unwrap();
        entry[3]
            .as_str()
            .and_then(|x| (x == "ground").then_some(Position::Grounded(time, lat, long)))
            .unwrap_or_else(|| {
                entry[3]
                    .as_f64()
                    .and_then(|x| {
                        Some(if x < 1000.0 {
                            Position::Grounded(time, lat, long)
                        } else {
                            Position::Flying(time, lat, long, x)
                        })
                    })
                    .unwrap_or(Position::Grounded(time, lat, long))
            })
    });

    let mut prev_position = positions.next().unwrap();
    let first = prev_position;
    let mut legs: Vec<Leg> = vec![];
    positions.for_each(|position| {
        match (prev_position, position) {
            (Position::Grounded(_, _, _), Position::Flying(_, _, _, _)) => {
                legs.push(Leg {
                    from: prev_position,
                    to: prev_position,
                });
            }
            (Position::Flying(_, _, _, _), Position::Grounded(_, _, _)) => {
                if legs.is_empty() {
                    legs.push(Leg {
                        from: first,
                        to: position,
                    })
                } else {
                    legs.last_mut().unwrap().to = position;
                }
            }
            _ => {}
        };
        prev_position = position;
    });
    assert!(!legs.is_empty()); // flying for more than 24h strait
    if matches!(prev_position, Position::Flying(_, _, _, _)) {
        legs.last_mut().unwrap().to = prev_position;
    }

    Ok(legs)
}
