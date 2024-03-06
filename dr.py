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
    , "count" AS (
        SELECT tail_number,year,to_airport, COUNT(*) AS count
        FROM "legs"
        GROUP BY tail_number,year,to_airport
    )
    , "ranked" AS (
        SELECT
        tail_number,year,to_airport,
        row_number() over (partition by tail_number, year order by count desc) as rank
        FROM "count"
    )
    , "top5" AS (
        SELECT * FROM "ranked" WHERE rank <= 5
    )
    , "top5_grouped" AS (
        SELECT tail_number, year, list(to_airport) as top_destinations FROM "top5" 
        GROUP BY tail_number, year
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
        "counts".*, "top5_grouped"."top_destinations"
    FROM "counts", "top5_grouped"
    WHERE "counts".tail_number = "top5_grouped".tail_number AND "counts".year = "top5_grouped".year
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
    , "count" AS (
        SELECT year,to_airport, COUNT(*) AS count
        FROM '{LEGS}'
        GROUP BY year,to_airport
    )
    , "ranked" AS (
        SELECT
        year,to_airport,
        row_number() over (partition by year order by count desc) as rank
        FROM "count"
    )
    , "top10" AS (
        SELECT * FROM "ranked" WHERE rank <= 10
    )
    , "top10_grouped" AS (
        SELECT year, list(to_airport) as top_destinations FROM "top10" 
        GROUP BY year
    )
    , "counts" AS (
        SELECT
            year
            , SUM(flying_time_hour) AS "flying_time_hour"
            , SUM(distance_km) AS "distance_km"
            , SUM(emissions_kg) AS "emissions_kg"
            , SUM(legs) AS "legs"
            , SUM(short_legs) AS "short_legs"
        FROM 'legs'
        GROUP BY year
    )
    SELECT 
        "counts".*, "top10_grouped"."top_destinations"
    FROM "counts", "top10_grouped"
    WHERE "counts".year = "top10_grouped".year
    ORDER BY "counts".year
)
TO 'results/dr_by_year.csv' (HEADER, DELIMITER ',');
"""
)
