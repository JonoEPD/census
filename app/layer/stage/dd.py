"""Ingestion of data dictionary for PUMA 2022."""

from glob import glob
import metadata
import pyspark.sql.functions as pys_fn

PUMS_DD_PATH = "layer/template/PUMS_data_dictionary.csv"

def ingest_data_dictionary(spark, table_name = "dd"):
    dd = spark.read.option("header", True).csv(f"{metadata.APP_PATH}/{PUMS_DD_PATH}")
    ldd = dd.withColumn("field", pys_fn.lower(dd.field))
    ldd.createOrReplaceTempView("raw_dd")
    cols = [ md.column for md in metadata.METADATA_LIST ]
    # handle nulls explicitly as null_str
    codes_df = spark.sql("""
    SELECT *, (n_codes_raw - has_null) n_codes
    FROM (
        SELECT field, ANY_VALUE(category) category, ARRAY_AGG(code) AS codes, ARRAY_AGG(text) AS texts, MAX(CASE WHEN code = "null_str" THEN 1 ELSE 0 END) has_null, COUNT(*) AS n_codes_raw
        FROM (
            SELECT field, category, text, (CASE WHEN code LIKE 'b%' THEN 'null_str' ELSE code END) code 
            FROM raw_dd WHERE heirarchy = 'VAL'
        ) 
        GROUP BY field
    )
    """)
    codes_df.createOrReplaceTempView("codes") # register to query again
    # map the different rows into categories 
    aug_codes_df = spark.sql("""
        SELECT *, CASE category WHEN 'N' THEN 'number' WHEN 'C' THEN
            CASE WHEN n_codes <= 2 THEN 'binary' WHEN n_codes <= 10 THEN 'small categorical' ELSE 'large categorical' END 
        ELSE 'ERROR' END cardinality, CAST(codes[has_null] AS int) lower_bound, CAST(codes[n_codes-1+has_null] AS int) upper_bound
        FROM codes
    """)
    aug_codes_df.createOrReplaceTempView("aug") # register to query again
    # join in text name since its helpful metadata
    names_df = spark.sql("""
        SELECT sdd.*, name_text FROM aug AS sdd INNER JOIN (
        SELECT DISTINCT field, name_text FROM raw_dd WHERE heirarchy='NAME'
        ) AS dd_name
        ON sdd.field = dd_name.field ORDER BY sdd.field
    """)
    # insert our cleaned metadata df
    names_df.writeTo(f"{metadata.CATALOG}.stage.dd").createOrReplace()
    return names_df
