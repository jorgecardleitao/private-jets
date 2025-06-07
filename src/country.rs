use std::{collections::HashMap, sync::Arc};

#[derive(Debug, serde::Deserialize, Clone)]
struct CountryRange {
    country: String,
    start: u32,
    end: u32,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct IcaoRange(u32, u32);

impl IcaoRange {
    /// Returns whether a valid `icao_number` in hex representation
    /// is part of this range. Returns false if the number is not valid
    fn contains(&self, icao_number: u32) -> bool {
        icao_number >= self.0 && icao_number <= self.1
    }
}

// note: the country.json was extracted from
// https://globe.adsbexchange.com/adsbx_comb_index_tarmisc_min_fc01616f370a6163a397b31cbee9dcd9.js on 2024-02-10
// which seems (and is likely to be) a correct implementation of https://www.icao.int/Meetings/AMC/MA/NACC_DCA03_2008/naccdca3wp05.pdf
// see tests below
static COUNTRIES: &'static [u8] = include_bytes!("./country.json");

#[derive(Debug, Clone, PartialEq)]
pub struct CountryIcaoRanges(HashMap<Arc<str>, IcaoRange>);

impl CountryIcaoRanges {
    /// Returns a new [`CountryRanges`] based on ICAO's mandatory guidelines,
    /// https://www.icao.int/Meetings/AMC/MA/NACC_DCA03_2008/naccdca3wp05.pdf
    /// Countries names are in ISO 3166.
    pub fn new() -> Self {
        let value: Vec<CountryRange> =
            serde_json::from_slice(COUNTRIES).expect("src/country.json to be deserializable");

        Self(
            value
                .into_iter()
                .map(|range| (range.country.into(), IcaoRange(range.start, range.end)))
                .collect(),
        )
    }

    /// Returns the country (ISO 3166) of the icao_number.
    /// `O(N)` where N is the number of countries in https://www.icao.int/Meetings/AMC/MA/NACC_DCA03_2008/naccdca3wp05.pdf
    pub fn country(&self, icao_number: &str) -> Result<Option<&Arc<str>>, String> {
        let Ok(icao_number_u32) = u32::from_str_radix(icao_number, 16) else {
            return Err(format!("{icao_number} is not in hex format"));
        };
        Ok(self
            .0
            .iter()
            .find_map(|(c, range)| range.contains(icao_number_u32).then_some(c)))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn positive() {
        assert_eq!(
            CountryIcaoRanges::new().country("458D6B"),
            Ok(Some(&"Denmark".into()))
        );
    }

    #[test]
    fn negative() {
        // exists in ads-b, but can't be assigned
        assert_eq!(CountryIcaoRanges::new().country("EA00CA"), Ok(None));
    }
}
