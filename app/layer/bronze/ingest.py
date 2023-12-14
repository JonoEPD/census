"""Select rows with correct types for census data."""

import metadata

def load_census(spark, metadatas= metadata.METADATA_LIST):
    """Ingest data from staging to bronze using the type definitions.
    
    Fairly simple logic, but separating the stage from bronze is helpful."""
    if spark.catalog.currentCatalog == "nessie": # create namespace for nessie
        ensure_staging_namespace()
    df = metadata.Metadata.generate_bronze_df(spark, metadatas=metadata.METADATA_LIST)
    query = df.writeTo(f"{metadata.TABLESPACE}.bronze.census").partitionedBy("YEAR")
    query.createOrReplace() # overwrite table if not exists

def ensure_staging_namespace(spark) -> None:
    """Create nessie namespace for stage."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {metadata.TABLESPACE}.bronze")

def set_comments(spark, metadatas = metadata.METADATA_LIST):
    """Appends comments via to the columns one by one ALTER TABLE ALTER COLUMN."""
    for query in metadata.Metadata.comment_queries(metadata.METADATA_LIST):
        spark.sql(query)