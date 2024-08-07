SET s3_endpoint='fra1.digitaloceanspaces.com';

COPY (
    SELECT
    "aircraft_model",
    "year",
    COUNT(*) AS "legs",
    SUM(epoch("end" - "start")) / 3600 AS "flight_time",
    SUM("hours_above_30000") / (SUM(epoch("end" - "start")) / 3600) AS "ratio_above_30000",
    SUM("hours_above_40000") / (SUM(epoch("end" - "start")) / 3600) AS "ratio_above_40000"
    FROM read_csv_auto("s3://private-jets/leg/v2.1/all/year=2023/data.csv", header = true)
    GROUP BY "aircraft_model", "year"
    ORDER BY "ratio_above_30000" DESC
) TO 'altitude.csv' (FORMAT 'csv');

COPY (
    SELECT model, AVG("gph") AS avg_gph, stddev_pop("gph") AS std_gph, stddev_pop("gph")/AVG("gph") AS ratio_gph
    FROM read_csv_auto("s3://private-jets/model/db/data.csv", header = true)
    GROUP BY model
    ORDER BY "ratio_gph" DESC
) TO 'variation.csv' (FORMAT 'csv');
