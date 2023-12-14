"""Script to ingest data into iceberg tables."""

from glob import glob
import pyspark.sql.functions as pys_fn
import metadata

PARTITION_COLS = ["YEAR", "ST"] # State and PUMA version of Area Code

def load_all_files_sequential_filtered(spark):
    """Execute more complex / less mem usage load. Try this if having mem issue loading the CSV (2GB)
    
    This function is also filtered by the metadata we will actually use."""
    first_insert_flag = True
    for yr in metadata.YEARS: # loop over years
        psam_files = glob(f"{metadata.DATA_PATH}/{yr}/1-Year/psam_*.csv")
        for file_name in psam_files: # loop over states
            df = spark.read.option("header", True).csv(file_name)
            df_f = df[metadata.INPUT_LIST]
            df_f_year = df_f.withColumn("YEAR", pys_fn.lit(yr))
            write_fn = df_f_year.writeTo(f"{metadata.CATALOG}.stage.census").partitionedBy(*PARTITION_COLS)
            if first_insert_flag == True:
                write_fn.createOrReplace()
                first_insert_flag = False
            else:
                write_fn.append()

def ensure_staging_namespace(spark) -> None:
    """Create nessie namespace for stage."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {metadata.CATALOG}.stage")

def load_all_files(spark):
    """Load everything. No filter on columns."""
    first_insert_flag = True
    for yr in metadata.YEARS:
        psam_files = glob(f"{metadata.DATA_PATH}/{yr}/1-Year/psam_*.csv")
        df = spark.read.option("header", True).csv(psam_files)
        df_yr = df.withColumn("YEAR", pys_fn.lit(yr))
        write_fn = df_yr.writeTo(f"{metadata.CATALOG}.stage.census").partitionedBy(*PARTITION_COLS)
        if first_insert_flag == True:
            write_fn.createOrReplace() # remove existing table if exists
            first_insert_flag = False
        else:
            write_fn.append()