"""Script to ingest data into iceberg tables."""

from glob import glob
from pyspark.sql.functions import lit
from . import metadata

PATH = "/home/iceberg/data/"
PARTITION_COLS = ["YEAR", "ST"] # State and PUMA version of Area Code

def load_all_files(spark):
    first_insert_flag = True
    for yr in metadata.YEARS: # loop over years
        psam_files = glob(f"/home/iceberg/data/{yr}/1-Year/psam_*.csv")
        for file_name in psam_files: # loop over states
            df = spark.read.option("header", True).csv(file_name)
            df_f = df[metadata.INPUT_LIST]
            df_f_year = df_f.withColumn("YEAR", lit(yr))
            write_fn = df_f_year.writeTo("demo.stage.census").partitionedBy(*PARTITION_COLS)
            if first_insert_flag == True:
                write_fn.createOrReplace()
                first_insert_flag = False
            else:
                write_fn.append()
