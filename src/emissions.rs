#[derive(Debug, Clone, Copy)]
pub enum Class {
    Economy,
    Business,
    First,
}

#[derive(Debug, Clone, Copy)]
pub enum Haul {
    Short,
    Long,
}

#[derive(Debug, Clone, Copy)]
#[allow(non_snake_case)]
struct Parameters {
    pub S: f64,
    pub PLF: f64,
    pub DC: f64,
    pub OneMinusCF: f64,
    pub CW: f64,
    pub EF: f64,
    pub P: f64,
    pub M: f64,
    pub AF: f64,
    pub A: f64,
    pub a: f64,
    pub b: f64,
    pub c: f64,
}

impl Parameters {
    /// exact parameters taken from https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/
    pub fn new(class: Class, haul: Haul) -> Self {
        match haul {
            Haul::Long => Parameters {
                S: 280.21,
                PLF: 0.82,
                DC: 95.0,
                OneMinusCF: 0.74,
                CW: match class {
                    Class::Economy => 0.8,
                    Class::Business => 1.54,
                    Class::First => 2.4,
                },
                EF: 3.15,
                P: 0.54,
                M: 2.0,
                AF: 0.00038,
                A: 11.68,
                a: 0.0001,
                b: 7.104,
                c: 5044.93,
            },
            Haul::Short => Parameters {
                S: 153.51,
                PLF: 0.82,
                DC: 95.0,
                OneMinusCF: 0.93,
                CW: match class {
                    Class::Economy => 0.96,
                    Class::Business => 1.26,
                    Class::First => 2.4,
                },
                EF: 3.15,
                P: 0.54,
                M: 2.0,
                AF: 0.00038,
                A: 11.68,
                a: 0.0,
                b: 2.714,
                c: 1166.52,
            },
        }
    }

    pub fn emissions(class: Class, haul: Haul, x: f64) -> f64 {
        let p = Parameters::new(class, haul);
        let x = x + p.DC;
        (p.a * x * x + p.b * x + p.c) / (p.S * p.PLF) * p.OneMinusCF * p.CW * (p.EF * p.M + p.P)
            + p.AF * x
            + p.A
    }
}

/// Performs operation defined at https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/
fn distance_to_emissions(distance: f64, class: Class) -> f64 {
    let mut haul = None;
    if distance < 1500.0 {
        haul = Some(Haul::Short)
    } else if distance > 2500.0 {
        haul = Some(Haul::Long)
    };
    match haul {
        Some(haul) => Parameters::emissions(class, haul, distance),
        None => {
            let short = Parameters::emissions(class, Haul::Short, 1500.0);
            let long = Parameters::emissions(class, Haul::Long, 2500.0);
            long + (2500.0 - distance) / (2500.0 - 1500.0) * (short - long)
        }
    }
}

/// Returns emissions of a commercial flight flying `from` -> `to`, in kg of eCO2.
/// The exact calculation is described here: https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/
pub fn emissions(from: (f64, f64), to: (f64, f64), class: Class) -> f64 {
    distance_to_emissions(super::distance(from, to), class)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn boundaries_work() {
        let class = Class::Business;
        assert!(distance_to_emissions(1500.0, class) - distance_to_emissions(1500.1, class) < 0.01);
        assert!(distance_to_emissions(2500.0, class) - distance_to_emissions(2500.1, class) < 0.01)
    }
}
