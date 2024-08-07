WITH "tmp" AS (
SELECT
    SUM(CASE WHEN "happened" = 'FlownWithLadd' THEN 1 ELSE 0 END) AS "with_ladd_count",
    SUM(CASE WHEN "happened" = 'FlownWithoutLadd' THEN 1 ELSE 0 END) AS "without_ladd_count"
FROM
    read_csv_auto("ladd_10.csv", header = true)
)
SELECT
"with_ladd_count" / ("without_ladd_count" + "with_ladd_count")
FROM "tmp"
