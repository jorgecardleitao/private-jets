use crate::Position;

/// Represents a leg, also known as a [non-stop flight](https://en.wikipedia.org/wiki/Non-stop_flight)
/// between two positions.
#[derive(Debug, Clone, PartialEq)]
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

    /// The total two-dimensional length of the leg in km
    pub fn length(&self) -> f64 {
        self.positions.windows(2).map(|w| w[0].distace(&w[1])).sum()
    }

    /// Leg duration
    pub fn duration(&self) -> time::Duration {
        self.to().datetime() - self.from().datetime()
    }

    pub fn from(&self) -> &Position {
        self.positions.first().unwrap()
    }

    pub fn to(&self) -> &Position {
        self.positions.last().unwrap()
    }
}

fn grounded_heuristic(previous_position: &Position, position: &Position) -> bool {
    let is_flying = previous_position.flying() || position.flying();
    if !is_flying {
        return false;
    }
    let lost_close_to_ground = position.datetime() - previous_position.datetime()
        > time::Duration::minutes(5)
        && (position.altitude() < 10000.0 || previous_position.altitude() < 10000.0);

    // lost signal for more than 10h => assume it landed somewhere
    let lost_somewhere =
        position.datetime() - previous_position.datetime() > time::Duration::hours(10);

    is_flying && (lost_close_to_ground || lost_somewhere)
}

/// Implementation of the definition of landed in [M-identify-legs](../methodology.md).
fn landed(previous_position: &Position, position: &Position) -> bool {
    (previous_position.flying() && position.grounded())
        || grounded_heuristic(previous_position, position)
}

fn is_grounded(previous_position: &Position, position: &Position) -> bool {
    (previous_position.grounded() && position.grounded())
        || grounded_heuristic(previous_position, position)
}

/// Iterator returning [`Leg`] computed according to the [methodology `M-identify-legs`](../methodology.md).
pub struct Legs<I: Iterator<Item = Position>> {
    positions: I,
    previous_position: Position,
    sequence: Vec<Position>,
}

impl<I: Iterator<Item = Position>> Legs<I> {
    fn new(mut positions: I) -> Self {
        let previous_position = positions.next().unwrap_or(Position {
            datetime: time::OffsetDateTime::from_unix_timestamp(0).unwrap(),
            latitude: 0.0,
            longitude: 0.0,
            altitude: None,
        });
        Self {
            positions,
            sequence: vec![],
            previous_position,
        }
    }
}

impl<I: Iterator<Item = Position>> Iterator for Legs<I> {
    type Item = Leg;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(position) = self.positions.next() {
            if !is_grounded(&self.previous_position, &position) {
                // it is flying -> add it to the sequence
                if self.sequence.is_empty() {
                    self.sequence.push(self.previous_position.clone());
                }
                self.sequence.push(position.clone());
            }
            if landed(&self.previous_position, &position) {
                if !self.sequence.is_empty() {
                    self.previous_position = position;
                    return Some(Leg {
                        positions: std::mem::take(&mut self.sequence),
                    });
                };
            }
            self.previous_position = position;
        }
        (!self.sequence.is_empty()).then_some(Leg {
            positions: std::mem::take(&mut self.sequence),
        })
    }
}

/// Returns a set of [`Leg`]s from a sequence of [`Position`]s according
/// to the [methodology `M-identify-legs`](../methodology.md).
pub fn legs(positions: impl Iterator<Item = Position>) -> impl Iterator<Item = Leg> {
    Legs::new(positions)
        // ignore legs that are too fast, as they are likely noise
        .filter(|leg| leg.duration() > time::Duration::minutes(5))
        // ignore legs that are too short, as they are likely noise
        .filter(|leg| leg.distance() > 3.0)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn positions() {
        assert_eq!(Leg { positions: vec![] }.positions(), &[]);
    }

    #[test]
    fn empty_leg() {
        assert_eq!(Legs::new(vec![].into_iter()).count(), 0);
    }

    fn test(positions: Vec<(i64, Option<f64>)>, expected: Vec<Vec<(i64, Option<f64>)>>) {
        let pos = |(t, altitude): (i64, Option<f64>)| Position {
            datetime: time::OffsetDateTime::from_unix_timestamp(t).unwrap(),
            latitude: 0.0,
            longitude: 0.0,
            altitude,
        };

        let legs = Legs::new(positions.into_iter().map(pos)).collect::<Vec<_>>();

        assert_eq!(
            legs,
            expected
                .into_iter()
                .map(|positions| Leg {
                    positions: positions.into_iter().map(pos).collect()
                })
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn basic() {
        test(
            vec![(0, None), (1, Some(2.1)), (2, Some(2.1)), (3, None)],
            vec![vec![(0, None), (1, Some(2.1)), (2, Some(2.1)), (3, None)]],
        );
    }

    #[test]
    fn grounded_is_ignored() {
        test(
            vec![
                (0, None), // should be ignored because next is on the ground
                (1, None),
                (10, Some(2.1)),
                (20, Some(2.1)),
                (30, None),
                (40, None), // should be ignored because previous is on the ground
                (50, None), // should be ignored because previous is on the ground
            ],
            vec![vec![
                (1, None),
                (10, Some(2.1)),
                (20, Some(2.1)),
                (30, None),
            ]],
        );
    }

    #[test]
    fn not_landed_is_a_leg() {
        test(
            vec![(0, None), (1, Some(2.1)), (2, Some(2.1))],
            vec![vec![(0, None), (1, Some(2.1)), (2, Some(2.1))]],
        );
    }

    #[test]
    fn low_and_5m_is_new_leg() {
        test(
            vec![
                (0, None),
                (10, Some(2.1)),
                (10 + 5 * 60 + 1, Some(2.1)), // >5m -> new leg
                (10 + 5 * 60 + 2, Some(2.1)),
                (10 + 5 * 60 + 3, Some(2.1)),
            ],
            vec![
                vec![(0, None), (10, Some(2.1))],
                vec![
                    (10 + 5 * 60 + 1, Some(2.1)),
                    (10 + 5 * 60 + 2, Some(2.1)),
                    (10 + 5 * 60 + 3, Some(2.1)),
                ],
            ],
        );
    }

    #[test]
    fn high_and_10h_is_new_leg() {
        // > 10k feet
        let alt = 10001f64;
        let delta = 10 * 60 * 60;
        test(
            vec![
                (0, None),
                (10, Some(alt)),
                (10 + delta + 1, Some(alt)), // >10h -> new leg
                (10 + delta + 2, Some(alt)),
                (10 + delta + 3, Some(alt)),
            ],
            vec![
                vec![(0, None), (10, Some(alt))],
                vec![
                    (10 + delta + 1, Some(alt)),
                    (10 + delta + 2, Some(alt)),
                    (10 + delta + 3, Some(alt)),
                ],
            ],
        );
    }
}
