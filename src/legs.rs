use crate::Position;

/// Represents a leg, also known as a [non-stop flight](https://en.wikipedia.org/wiki/Non-stop_flight)
/// between two positions.
#[derive(Debug, Clone)]
pub struct Leg {
    /// Sequence of positions defining the leg. Ends may start Flying, when the first/last observed
    /// position was flying. Otherwise, first and last are Grounded.
    positions: Vec<Position>,
}

impl Leg {
    /// Positions of the leg
    pub fn positions(&self) -> &[Position] {
        &self.positions
    }

    /// Leg geo distance in km
    pub fn distance(&self) -> f64 {
        self.from().distace(&self.to())
    }

    /// Leg duration
    pub fn duration(&self) -> time::Duration {
        self.to().datetime() - self.from().datetime()
    }

    pub fn maximum_altitude(&self) -> f64 {
        self.positions
            .iter()
            .map(|p| p.altitude() as u32)
            .max()
            .unwrap() as f64
    }

    pub fn from(&self) -> &Position {
        self.positions.first().unwrap()
    }

    pub fn to(&self) -> &Position {
        self.positions.last().unwrap()
    }
}

/// Returns a set of [`Leg`]s from a sequence of [`Position`]s.
pub fn all_legs(positions: impl Iterator<Item = Position>) -> Vec<Leg> {
    // M-3
    fn _correct_low_flying(position: Position) -> Position {
        if position.altitude() < 1000.0 {
            Position::Grounded {
                icao: position.icao().clone(),
                datetime: position.datetime(),
                latitude: position.latitude(),
                longitude: position.longitude(),
            }
        } else {
            position
        }
    }
    let mut positions = positions.map(_correct_low_flying);

    let Some(mut prev_position) = positions.next() else {
        return vec![];
    };

    let mut sequence: Vec<Position> = vec![];
    let mut legs: Vec<Leg> = vec![];
    positions.for_each(|position| {
        if let (Position::Grounded { .. }, Position::Grounded { .. }) = (&prev_position, &position)
        {
            prev_position = position;
            return;
        };
        sequence.push(position.clone());
        if let (Position::Flying { .. }, Position::Grounded { .. }) = (&prev_position, &position) {
            legs.push(Leg {
                positions: std::mem::take(&mut sequence),
            });
        };
        prev_position = position;
    });

    // if it is still flying, remove the incomplete leg
    if matches!(prev_position, Position::Flying { .. }) && !legs.is_empty() {
        legs.pop();
    }

    legs
}

/// Returns a set of [`Leg`]s from a sequence of [`Position`]s according
/// to the [methodology `M-4`](../methodology.md).
pub fn legs(positions: impl Iterator<Item = Position>) -> Vec<Leg> {
    all_legs(positions)
        .into_iter()
        // ignore legs that are too fast, as they are likely noise
        .filter(|leg| leg.duration() > time::Duration::minutes(5))
        // ignore legs that are too short, as they are likely noise
        .filter(|leg| leg.distance() > 3.0)
        .collect()
}
