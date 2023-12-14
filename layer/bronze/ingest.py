"""Select rows with correct types for census data."""

import metadata

def load_census(spark, metadatas= metadata.METADATA_LIST):
    """Ingest data from staging to bronze using the type definitions.
    
    Fairly simple logic, but separating the stage from bronze is helpful."""
    df = metadata.Metadata.generate_bronze_df(spark, metadatas=metadata.METADATA_LIST)
    query = df.writeTo("demo.bronze.census").partitionedBy("YEAR")
    query.createOrReplace() # overwrite table if not exists

def set_comments(spark, metadatas = metadata.METADATA_LIST):
    """Appends comments via to the columns one by one ALTER TABLE ALTER COLUMN."""
    for query in metadata.Metadata.comment_queries(metadata.METADATA_LIST):
        spark.sql(q)