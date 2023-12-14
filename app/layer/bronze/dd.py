import metadata

def ingest_data_dictionary(spark, table_name = "dd"):
    """Simple op to move data dictionary to bronze."""
    dd_df = spark.table(f"{metadata.CATALOG}.stage.{table_name}")
    dd_df.writeTo(f"{metadata.CATALOG}.bronze.{table_name}").createOrReplace()
    return dd_df # can ignore