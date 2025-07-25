--SET s3_endpoint='fra1.digitaloceanspaces.com';

load spatial;

WITH leg AS (
    SELECT * FROM read_csv_auto("results/data.csv", header = true)
    LIMIT 1000
)

, region AS (
    SELECT * FROM ST_Read("world.json")
)

SELECT country, COUNT(*), SUM(co2_emissions), SUM(distance), SUM(great_circle_distance)
FROM leg, region
WHERE ST_Within(ST_Transform(ST_Point(end_lon, end_lat), 'EPSG:4326', 'WGS84'), geom)
GROUP BY country
