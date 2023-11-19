use crate::Position;

/// Represents a leg, also known as a [non-stop flight](https://en.wikipedia.org/wiki/Non-stop_flight)
/// between two positions.
#[derive(Debug, Clone, Copy)]
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

    let first = prev_position;
    let mut legs: Vec<Leg> = vec![];
    let mut maximum_altitude = prev_position.altitude();
    positions.for_each(|position| {
        maximum_altitude = position.altitude().max(maximum_altitude);
        match (prev_position, position) {
            (Position::Grounded { .. }, Position::Flying { .. }) => {
                // departed, still do not know to where
                legs.push(Leg {
                    from: prev_position,
                    to: prev_position,
                    maximum_altitude,
                });
            }
            (Position::Flying { .. }, Position::Grounded { .. }) => {
                // arrived
                if let Some(leg) = legs.last_mut() {
                    // there is a leg - set its arrival position
                    leg.to = position;
                } else {
                    // if it was initially flying, need to push to the leg
                    legs.push(Leg {
                        from: first,
                        to: position,
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
