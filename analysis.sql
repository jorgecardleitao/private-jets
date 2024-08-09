SET s3_endpoint='fra1.digitaloceanspaces.com';

SELECT
    "year",
    COUNT(DISTINCT("tail_number")) AS "number of aircrafts",
    COUNT(*) AS "number of legs",
    SUM("co2_emissions")/1000/1000/1000 AS "Mt CO2 emitted",
    SUM("distance")/1000/1000/1000 AS "M km flown",
    SUM("great_circle_distance")/1000/1000/1000 AS "M km traveled",
    SUM("duration")/24 AS "days flown",
    SUM("hours_above_30000")/24 AS "days above 30k feet",
    SUM("hours_above_40000")/24 AS "days above 40k feet",
FROM read_csv_auto("s3://private-jets/leg/v2/all/year=*/data.csv", header = true)
WHERE "start" < DATE '2024-06-01'
GROUP BY "year"
ORDER BY "year"
