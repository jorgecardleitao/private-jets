SET s3_endpoint='fra1.digitaloceanspaces.com';
-- calculates the distance between two points on Earth (https://rosettacode.org/wiki/Haversine_formula)
CREATE MACRO distance(start_latitude, start_longitude, end_latitude, end_longitude) AS (
    6372.8 * 2 * asin(sqrt(sin(radians(end_latitude - start_latitude) / 2)**2 + cos(radians(start_latitude)) * cos(radians(end_latitude)) * sin(radians(end_longitude - start_longitude) / 2)**2))
);
-- calculates emissions using M-emissions-private-jet
CREATE MACRO leg_tons_co2e(gph, leg_h) AS (
    gph       -- [gallon/h]
    * 3.78541 -- liters / gallons [L/gallon]
    * 0.8     -- liters to kg of jet fuel [L/kg]
    * 3.16    -- emissions per kg [kg CO2 / kg jet fuel]
    * 3.0     -- Radiative Forcing index [kg CO2e / kg CO2]
    * 1.68    -- Radiative Forcing index [kg CO2e / kg CO2]
    * leg_h   -- leg time [h]
    / 1000    -- tons
);
-- used to avoid downloading the data on every run
--COPY
--    (SELECT *
--    FROM read_csv_auto("s3://private-jets/leg/v2/all/year=*/data.csv", header = true))
--TO 'results/leg/' (FORMAT 'parquet', PARTITION_BY "year");
COPY (
    -- set of models for private use and corresponding gph averaged over sources
    WITH "private_jet_model" AS (
        SELECT model, AVG(gph) AS gph
        FROM read_csv_auto("s3://private-jets/model/db/data.csv", header = true)
        GROUP BY model
    )
    -- all legs from all icao numbers of private jets
    , "leg" AS (
        SELECT *, "end" - "start" AS duration
        FROM 'results/leg/year=**/*.parquet'
    )
    , "aircraft" AS (
        SELECT *
        -- this uses a fixed time, but the correct way is to get all and compute the file closest to the day of the leg.
        FROM read_csv_auto("s3://private-jets/aircraft/db/date=2024-06-25/data.csv", header = true)
    )
    , "private_jet" AS (
        SELECT "aircraft".*, "gph"
        FROM "aircraft"
        JOIN "private_jet_model" ON "aircraft"."model" = "private_jet_model"."model"
    )
    , "private_jet_leg" AS (
        SELECT "tail_number", "model", "country", "leg".*, "gph"
        FROM "leg"
        JOIN "private_jet" ON "leg"."icao_number" = "private_jet"."icao_number"
    )
    SELECT
        "year",
        country,
        tail_number,
        model,
        COUNT(*) AS "flights",
        SUM(epoch(duration)) / 3600 AS "flying_time_h",
        SUM(distance("start_lat", "start_lon", "end_lat", "end_lon")) AS "flying_distance_km",
        SUM("length") AS "flying_length_km",
        SUM(leg_tons_co2e(gph, epoch(duration) / 3600)) AS "emissions_co2e_tons",
    FROM "private_jet_leg"
    WHERE
        -- exceptions that seem to be incorrectly assigned in ADS-B db
        (tail_number, year) NOT IN (('C-GSAT', 2019), ('C-GSAT', 2020), ('C-GSAT', 2021), ('C-FOGT', 2019))
        AND tail_number NOT IN ('VQ-BIO', 'C-GPAT')
    GROUP BY year, country, tail_number, model
    ORDER BY "emissions_co2e_tons" DESC
)
TO 'all.csv' (FORMAT 'csv');
