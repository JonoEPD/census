"""Select rows with correct types for census data."""

import metadata
import pyspark.sql.functions as pys_fn

def load_census(spark, metadatas= metadata.METADATA_LIST):
    """Ingest data from staging to bronze using the type definitions.
    
    Fairly simple logic, but separating the stage from bronze is helpful."""
    df = metadata.generate_bronze_df(spark, metadatas)
    df_id = df.withColumn("id", pys_fn.monotonically_increasing_id())
    plan = df_id.writeTo(f"{metadata.CATALOG}.bronze.census").partitionedBy("year")
    plan.createOrReplace() # overwrite table if not exists

def ensure_staging_namespace(spark) -> None:
    """Create nessie namespace for stage."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {metadata.TABLESPACE}.bronze")

def set_comments(spark, metadatas = metadata.METADATA_LIST):
    """Appends comments via to the columns one by one ALTER TABLE ALTER COLUMN."""
    for query in metadata.comment_queries(metadata.METADATA_LIST):
        spark.sql(query)