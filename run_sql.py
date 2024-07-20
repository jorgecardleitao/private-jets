import sys
import duckdb

with open(sys.argv[1]) as f:
    sql = f.read()


print(duckdb.sql(sql))
