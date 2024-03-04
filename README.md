# Private jet flights
[![Test](https://github.com/jorgecardleitao/private-jets/actions/workflows/test.yaml/badge.svg)](https://github.com/jorgecardleitao/private-jets/actions/workflows/test.yaml)
[![Coverage](https://codecov.io/gh/jorgecardleitao/private-jets/graph/badge.svg?token=DT7C376OKH)](https://codecov.io/gh/jorgecardleitao/private-jets)

This repository contains a CLI application to analyze flights of private jets.

It is supported by an S3 Blob storage container for caching data, thereby
reducing its impact to [https://adsbexchange.com/](https://adsbexchange.com/).

## How to use the data

This solution's data is publicly available over https and s3 protocols.
The data format is always either CSV or JSON as it offers the greatest compatibility.

### Examples

The following examples use [`duckDB`](https://duckdb.org/).
Similar functionality can be obtained with other query engines.

#### Number of legs and emissions

```python
import duckdb

# Path to all legs
# Not all data is still available - use years that are completed.
# See https://private-jets.fra1.digitaloceanspaces.com/leg/v1/status.json
# for the current status
LEGS_PATH = [
    #"https://private-jets.fra1.digitaloceanspaces.com/leg/v1/all/year=2019/data.csv",
    "https://private-jets.fra1.digitaloceanspaces.com/leg/v1/all/year=2020/data.csv",
    #"https://private-jets.fra1.digitaloceanspaces.com/leg/v1/all/year=2021/data.csv",
    #"https://private-jets.fra1.digitaloceanspaces.com/leg/v1/all/year=2022/data.csv",
    "https://private-jets.fra1.digitaloceanspaces.com/leg/v1/all/year=2023/data.csv",
]

print(duckdb.sql(
    f"""
SELECT 
    year
    , SUM(distance) AS "distance_km"
    , SUM(duration) AS "flying_time_km"
    , COUNT(*) AS "legs"
    , SUM(emissions_kg) / 1000 / 1000 / 1000 AS "emissions_mt"
FROM read_csv_auto({LEGS_PATH}, header = true)
GROUP BY year
ORDER BY year
"""
))
```

### Datasets

#### DATASET-1 - List of private jets

> `https://private-jets.fra1.digitaloceanspaces.com/private_jets/all.csv`

#### DATASET-2 - Positions of an ICAO number (as per ADS-B exchange)

> `https://private-jets.fra1.digitaloceanspaces.com/globe_history/{date}/trace_full_{icao_number}.json`

where `{icao_number}` is the icao number in lower case (e.g. `06a0d8`) and `{date}` the date in ISO 8601 (e.g. `2019-01-01`).

This dataset is a one-to-one (binary) representation of ADS-B exchange history endpoint.

#### DATASET-3 - Positions of an ICAO number by month

> `https://private-jets.fra1.digitaloceanspaces.com/position/icao_number={icao_number}/month={month}/data.json`

where `{icao_number}` is the icao number in lower case (e.g. `06a0d8`) and `{month}` the
month in ISO 8601 (e.g. `2019-01`).

This dataset is an aggregation of `DATASET-2` over months.

#### DATASET-4 - Legs of an ICAO number by month

> `https://private-jets.fra1.digitaloceanspaces.com/leg/v1/data/icao_number={icao_number}/month={month}/data.csv`

where `{icao_number}` is the icao number in lower case (e.g. `06a0d8`) and `{month}` the
month in ISO 8601 (e.g. `2019-01`).

This is the result of applying the methodology `M-4` to the dataset `DATASET-3`.

#### DATASET-5 - Legs of an ICAO number by year

> `https://private-jets.fra1.digitaloceanspaces.com/leg/v1/all/year={year}/data.csv`

where `{year}` is the year in ISO 8601 (e.g. `2019`).

This dataset is an aggregation of `DATASET-4` over months and aircrafts.

> `https://private-jets.fra1.digitaloceanspaces.com/leg/v1/status.json`

Exposes the current status of this calculation, as not all years are completed yet.

## How to use the code

### Risk and impact

This code performs API calls to [https://adsbexchange.com/](https://adsbexchange.com/),
a production website of a company.

**Use critical thinking** when using this code and how it impacts them.

We strongly recommend that if you plan to perform large scale analysis (e.g. in time or aircrafts),
that you reach out via an issue _before_, so that we can work together
to cache all hits to [https://adsbexchange.com/](https://adsbexchange.com/)
on an horizontally scaled remote storage and therefore remove its impact to adsbexchange.com
of future calls.

All cached data is available on S3 blob storage at endpoint

> `https://private-jets.fra1.digitaloceanspaces.com`

and has anonymous and public read permissions.

### How to use

This repository contains both a Rust library and a set of [`examples/`](./examples) used
to perform actual calculations. To use one of such examples:

1. Install Rust
2. run `cargo run --features="build-binary" --bin single_day -- --tail-number "OY-GFS" --date "2023-10-20"`
3. open `OY-GFS_2023-10-20_0.md`

Step 2. has an optional arguments, `--access-key`, `--secret-access-key`, specifying
credentials to write to the remote storate, as opposed to disk.

Finally, setting `--backend disk` ignores the remote storage altogether and
only uses disk for caching (resulting in higher cache misses and thus more
interactions with ADS-B exchange).

In general:
* Use the default parameters when creating ad-hoc stories
* Use `--access-key` when improving the database with new data.
* Use `--backend disk` when testing the caching system

As of today, the flag `--access-key` is only available when the code is executed
from `main`, as writing to the blob storage must be done through a controlled code base
that preserves data integrity.

### Examples:

```bash
# Story about Danish private jets that flew to Davos between two dates
cargo run --features="build-binary" --bin country -- --from=2024-01-13 --to=2024-01-21 --country=denmark --location=davos
# Story about Danish private jets that flew between two dates
cargo run --features="build-binary" --bin country -- --from=2024-01-13 --to=2024-01-21 --country=denmark
# Story about Portuguese private jets that flew between two dates
cargo run --features="build-binary" --bin country -- --from=2024-01-13 --to=2024-01-21 --country=portugal

# Story about German private jets that flew in 2023, where secret is on a file
cargo run --features="build-binary" --bin country -- --from=2023-01-01 --to=2024-01-01 --country=germany --access-key=DO00AUDGL32QLFKV8CEP --secret-access-key=$(cat secrets.txt)

# Build database of positions `[2020, 2023]`
cargo run --features="build-binary" --release --bin etl_positions -- --access-key=DO00AUDGL32QLFKV8CEP --secret-access-key=$(cat secrets.txt)
# they are available at
# https://private-jets.fra1.digitaloceanspaces.com/position/icao_number={icao}/month={year}-{month}/data.json

# Build database of legs `[2020, 2023]` (over existing positions computed by `etl_positions`)
cargo run --features="build-binary" --release --bin etl_legs -- --access-key=DO00AUDGL32QLFKV8CEP --secret-access-key=$(cat secrets.txt)
# they are available at
# https://private-jets.fra1.digitaloceanspaces.com/leg/v1/data/icao_number={icao}/month={year}-{month}/data.csv
```

## Methodology

The methodology used to extract information is available at [`methodology.md`](./methodology.md).

## Generated datasets

### Set of worldwide aicrafts whose primary use is to be a private jet:

* [Data](https://private-jets.fra1.digitaloceanspaces.com/private_jets/2023/11/06/data.csv)
* [Description](https://private-jets.fra1.digitaloceanspaces.com/private_jets/2023/11/06/description.md)
