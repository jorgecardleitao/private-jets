use std::collections::HashMap;

/// Returns the ISO 8601 representation of a month ("2023-01")
pub fn month_to_part(date: time::Date) -> String {
    format!("{}-{:02}", date.year(), date.month() as u8)
}

/// Parses a "2022-01" to a date at first of month
pub fn parse_month(date: &str) -> time::Date {
    time::Date::from_calendar_date(
        date[..4].parse().expect(&date[0..4]),
        date[5..7]
            .parse::<u8>()
            .expect(&date[5..7])
            .try_into()
            .unwrap(),
        1,
    )
    .unwrap()
}

pub fn hive_to_map<'a>(mut blob: &'a str) -> HashMap<&'a str, &'a str> {
    let mut a = HashMap::new();
    while !blob.is_empty() {
        let position = blob.find("=").unwrap();
        let key = &blob[..position];
        blob = &blob[position + 1..];
        let end = blob.find("/").unwrap_or(blob.len());
        let value = &blob[..end];
        blob = &blob[end + 1..];
        a.insert(key, value);
    }
    a
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn work() {
        let a = hive_to_map("a=1/b=2/");
        assert_eq!(a, vec![("a", "1"), ("b", "2")].into_iter().collect());
    }
}
