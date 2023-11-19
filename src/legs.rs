use crate::Position;

/// Represents a leg, also known as a [non-stop flight](https://en.wikipedia.org/wiki/Non-stop_flight)
/// between two positions.
#[derive(Debug, Clone, Copy)]
pub struct Leg {
    pub from: Position,
    pub to: Position,
}

/// Returns a set of [`Leg`]s from a sequence of [`Position`]s.
pub fn legs(mut positions: impl Iterator<Item = Position>) -> Vec<Leg> {
    let Some(mut prev_position) = positions.next() else {
        return vec![];
    };

    let first = prev_position;
    let mut legs: Vec<Leg> = vec![];
    positions.for_each(|position| {
        match (prev_position, position) {
            (Position::Grounded { .. }, Position::Flying { .. }) => {
                // departed, still do not know to where
                legs.push(Leg {
                    from: prev_position,
                    to: prev_position,
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
                    })
                }
            }
            _ => {}
        };
        prev_position = position;
    });

    // if it is still flying, we leave the last leg as incomplete.
    if matches!(prev_position, Position::Flying { .. }) && !legs.is_empty() {
        legs.last_mut().unwrap().to = prev_position;
    }

    legs
}
