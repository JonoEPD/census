"""Functions to define how to preprocess various ACS fields. 

General behavior in phase 1 of preprocessing:
- sensitive vars => no change
- boolean => values (1,2) mapped to 1, 0
- decimal => integer representation divided to correct decimal
- categorical (small cardinality) => one hot encoding
- categorical (large cardinality) => no change
"""

import metadata 
import numpy as np

def query_smallcat(sc_dd):
    unique_fields = np.unique(sc_dd['field'])
    query = "SELECT YEAR, PUMA"
    for field in unique_fields:
        if field == 'REGION': # FIXME
            continue
        query += f",\n{field}"
    for _, row in sc_dd.iterrows():
        if row['field'] == 'REGION': # FIXME
            continue
        silver_column, field, code, text = row.values
        match_str = '= ' + str(code) if not np.isnan(code) else 'IS NULL'
        query += f",\nCASE WHEN {field} {match_str} THEN 1 ELSE 0 END {silver_column}"
    query += "\nFROM test.bronze.census"
    return query

def create_smallcat(spark):
    sc_dd = spark.table("test.silver.md.smallcat").toPandas()
    sc_query = query_smallcat(sc_dd)
    return spark.sql(sc_query).

def run_all(spark, sc_dd, write=False):
    sc_df = create_smallcat(spark)

    if write:
        sc_dd.writeTo("test.silver.data.smallcat").createOrReplace()

    return sc_df