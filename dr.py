"""
How to use:

```
# start a venv
# (e.g. `python3 -m venv venv && source venv/bin/activate` on a Linux)
pip install duckdb
mkdir results
python dr.py
# see results in `results/` directory
```
"""

import duckdb

# Path to all aircrafts
AIRCRAFTS_PATH = "https://private-jets.fra1.digitaloceanspaces.com/private_jets/all.csv"
# Models that are turboprops
EXCLUDED_MODELS = {"PILATUS PC-24", "PILATUS PC-12", "PIPER PA-46-500TP"}
EXCLUDED_MODELS_SQL = ",".join([f"'{a}'" for a in EXCLUDED_MODELS])

# Path to all legs
LEGS_PATH = [
    "https://private-jets.fra1.digitaloceanspaces.com/leg/v1/all/year=2019/data.csv",
    "https://private-jets.fra1.digitaloceanspaces.com/leg/v1/all/year=2020/data.csv",
    "https://private-jets.fra1.digitaloceanspaces.com/leg/v1/all/year=2021/data.csv",
    "https://private-jets.fra1.digitaloceanspaces.com/leg/v1/all/year=2022/data.csv",
    "https://private-jets.fra1.digitaloceanspaces.com/leg/v1/all/year=2023/data.csv",
]

AIRCRAFTS = "results/dr_aircrafts.csv"
LEGS = "results/dr_legs.csv"
AIRPORTS = "results/airports.csv"


duckdb.sql(
    f"""
COPY (
    SELECT *
    FROM read_csv_auto({LEGS_PATH}, header = true)
    WHERE tail_number LIKE 'OY-%'
    AND model NOT IN ({EXCLUDED_MODELS_SQL})
    ORDER BY tail_number, start
)
TO '{LEGS}' (HEADER, DELIMITER ',')
"""
)
duckdb.sql(
    f"""
COPY (
    SELECT icao_number, tail_number, model
    FROM read_csv_auto('{AIRCRAFTS_PATH}', header = true)
    WHERE tail_number LIKE 'OY-%'
    AND model NOT IN ({EXCLUDED_MODELS_SQL})
    ORDER BY tail_number
)
TO '{AIRCRAFTS}' (HEADER, DELIMITER ',')
"""
)

duckdb.sql(
    f"""
COPY (
    WITH "legs" AS (
        SELECT *
        FROM '{LEGS}'
    )
    , "counts" AS (
        SELECT
            tail_number
            , year
            , SUM(duration) AS "flying_time_hour"
            , SUM(distance) AS "distance_km"
            , SUM(emissions_kg) AS "emissions_kg"
            , COUNT(*) AS "legs"
            , SUM(CAST(distance < 100 AS INT)) AS "short_legs"
        FROM 'legs'
        GROUP BY tail_number, year
    )
    SELECT 
        "counts".*
    FROM "counts"
    ORDER BY "counts".tail_number, "counts".year
)
TO 'results/dr_by_tail_number.csv' (HEADER, DELIMITER ',');
"""
)


duckdb.sql(
    f"""
COPY (
    WITH "legs" AS (
        SELECT *
        FROM 'results/dr_by_tail_number.csv'
    )
    , "counts" AS (
        SELECT
            year
            , SUM(flying_time_hour) AS "flying_time_hour"
            , SUM(distance_km) AS "distance_km"
            , SUM(emissions_kg) AS "emissions_kg"
            , COUNT(*) AS "legs"
            , SUM("short_legs") AS "short_legs"
        FROM 'legs'
        GROUP BY year
    )
    SELECT 
        "counts".*
    FROM "counts"
    ORDER BY "counts".year
)
TO 'results/dr_by_year.csv' (HEADER, DELIMITER ',');
"""
)
