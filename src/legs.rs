use crate::Position;

/// Represents a leg, also known as a [non-stop flight](https://en.wikipedia.org/wiki/Non-stop_flight)
/// between two positions.
#[derive(Debug, Clone)]
pub struct Leg {
    pub from: Position,
    pub to: Position,
    /// in feet
    pub maximum_altitude: f64,
}

impl Leg {
    /// Leg geo distance in km
    pub fn distance(&self) -> f64 {
        self.from.distace(&self.to)
    }

    /// Leg duration
    pub fn duration(&self) -> time::Duration {
        self.to.datetime() - self.from.datetime()
    }
}

/// Returns a set of [`Leg`]s from a sequence of [`Position`]s.
pub fn legs(mut positions: impl Iterator<Item = Position>) -> Vec<Leg> {
    let Some(mut prev_position) = positions.next() else {
        return vec![];
    };

    let first = prev_position.clone();
    let mut legs: Vec<Leg> = vec![];
    let mut maximum_altitude = first.altitude();
    positions.for_each(|position| {
        maximum_altitude = position.altitude().max(maximum_altitude);
        match (&prev_position, &position) {
            (Position::Grounded { .. }, Position::Flying { .. }) => {
                // departed, still do not know to where
                legs.push(Leg {
                    from: prev_position.clone(),
                    to: prev_position.clone(),
                    maximum_altitude,
                });
            }
            (Position::Flying { .. }, Position::Grounded { .. }) => {
                // arrived
                if let Some(leg) = legs.last_mut() {
                    // there is a leg - set its arrival position
                    leg.to = position.clone();
                } else {
                    // if it was initially flying, need to push to the leg
                    legs.push(Leg {
                        from: first.clone(),
                        to: position.clone(),
                        maximum_altitude,
                    });
                    maximum_altitude = 0.0
                }
            }
            _ => {}
        };
        prev_position = position;
    });

    // if it is still flying, remove the incomplete leg
    if matches!(prev_position, Position::Flying { .. }) && !legs.is_empty() {
        legs.pop();
    }

    legs
}

/// Computes legs that, under the below heuristic, is a real leg:
/// * Its maximum altitude is higher than 1000 feet
/// * Its distance is higher than 3km
/// * Its duration is longer than 5m
pub fn real_legs(positions: impl Iterator<Item = Position>) -> Vec<Leg> {
    legs(positions)
        .into_iter()
        // ignore legs that are too fast, as they are likely noise
        .filter(|leg| leg.duration() > time::Duration::minutes(5))
        // ignore legs that are too short, as they are likely noise
        .filter(|leg| leg.distance() > 3.0)
        // ignore legs that are too low, as they are likely noise
        .filter(|leg| leg.maximum_altitude > 1000.0)
        .collect()
}
