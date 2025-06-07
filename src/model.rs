use std::{collections::HashMap, error::Error, sync::Arc};

use serde::{Deserialize, Serialize};

/// A map of the aircraft model (e.g. `BEECH 400 Beechjet`) to an [`AircraftModel`].
pub type AircraftModels = HashMap<String, Arc<AircraftModel>>;

/// In-memory representation of an aircraft model
#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct AircraftModel {
    /// the model (e.g. `BEECH 400 Beechjet`)
    pub model: String,
    /// the consumption in gallons per hour
    pub gph: u32,
    /// the source that identifies it as a private jet
    pub source: String,
    /// the date of when the source was retrieved
    pub date: String,
}

static MODELS: &'static [u8] = include_bytes!("./models.csv");

/// Returns the set of all [`AircraftModel`] in `src/models.csv`,
/// corresponding to aircraft types whose primary use is to be a private jet
/// according to the [methodology `M-models-for-private-use`](../methodology.md).
/// The gph of each model is the average over all sources as per [methodology `M-average-consumption`](../methodology.md).
/// # Error
/// Errors if the file cannot be read
pub fn load_private_jet_models() -> Result<AircraftModels, Box<dyn Error>> {
    let data = super::csv::deserialize(MODELS)
        .map(|x| x.unwrap())
        .map(|a: AircraftModel| (a.clone(), a))
        .collect::<Vec<_>>();

    let data = data
        .into_iter()
        .fold(
            HashMap::<String, (AircraftModel, u32)>::default(),
            |mut acc, (a, b)| {
                // a == b in this case
                acc.entry(a.model)
                    .and_modify(|x: &mut (AircraftModel, u32)| {
                        x.0.source.push(';');
                        x.0.source.push_str(&a.source);
                        x.0.date.push(';');
                        x.0.date.push_str(&a.date);
                        x.0.gph += a.gph;
                        x.1 += 1;
                    })
                    .or_insert((b, 1));
                acc
            },
        )
        .into_iter()
        .map(|(model, (mut all, count))| {
            all.gph /= count;
            (model, Arc::new(all))
        })
        .collect();

    Ok(data)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn models() {
        let models = load_private_jet_models().unwrap();

        assert_eq!(
            models.get("GULFSTREAM 5").unwrap().gph,
            (500 + 430 + 455 + 438) / 4
        );
        let mut models = models.values().collect::<Vec<_>>();
        models.sort_unstable_by_key(|m| &m.model);
        let data_csv = crate::csv::serialize(models.into_iter());
        std::fs::write("models.csv", data_csv).unwrap();
    }
}
