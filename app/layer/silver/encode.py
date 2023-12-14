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
from pyspark.sql.functions import when, col

SILVER_DATA_PATH = f"{metadata.CATALOG}.silver.data"
SILVER_METADATA_PATH = f"{metadata.CATALOG}.silver.md"
BRONZE_DATA_PATH = "test.bronze.census"

def build_binary_select(df, dd):
    """Helper to generate a select statement for a table. 
    
    Using df query builder style (like sqlalchemy)
    
    Args:
        df: DataFrame to select on
        dd: Data Dictionary
        column_name: The column in the Data Dictionary with the df columns
    """
    pdf = dd.toPandas()
    cols = pdf['field'].values
    for _, row in pdf.iterrows():
        f = row['field']
        ct = row['code_true']
        cf = row['code_false']
        df = df.withColumn( # case statement. note that iceberg boolean only has 1/0
            f, when(col(f) == ct, 1).when(col(f) == cf, 0).otherwise("NULL")
        )
    return df.select(['id'] + [col for col in cols])

def query_smallcat(sc_dd):
    """Loop over the categorical fields with small cardinality (sc) to generate a query.

    Using string query builder style (literally create the query as a big string)
    
    Result is one new column per category.
    """
    sc_pdd = sc_dd.toPandas()
    unique_fields = np.unique(sc_pdd['field'])
    query = f"SELECT {', '.join(metadata.SILVER_IDENT_COLS)}"
    for field in unique_fields:
        if field == 'region': # FIXME
            continue
        query += f",\n{field}"
    for _, row in sc_pdd.iterrows():
        if row['field'] == 'region': # FIXME
            continue
        silver_column, field, code, text = row.values
        match_str = '= ' + str(code) if not np.isnan(code) else 'IS NULL'
        query += f",\nCASE WHEN {field} {match_str} THEN 1 ELSE 0 END {silver_column}"
    query += f"\nFROM {BRONZE_DATA_PATH}"
    return query

def get_data_dictionary(spark, table_name):
    return spark.table(f"{SILVER_METADATA_PATH}.{table_name}")

def create_smallcat(spark, _df, table_name = "smallcat"):
    """Create small categorical tables."""
    sc_dd  = get_data_dictionary(spark, table_name)
    sc_query = query_smallcat(sc_dd)
    return spark.sql(sc_query), f"{SILVER_DATA_PATH}.{table_name}"

def create_number(spark, df, table_name = "number"):
    """Create number tables. Copy of largecat. Might refine later to ensure min/max"""
    n_dd  = get_data_dictionary(spark, table_name)
    n_cols = n_dd.select(n_dd.field).toPandas()['field']
    return df.select(["id"] + [col for col in n_cols]), f"{SILVER_DATA_PATH}.{table_name}"

def create_largecat(spark, df, table_name = "largecat"):
    """Create large categorical tables."""
    n_dd  = get_data_dictionary(spark, table_name)
    n_cols = n_dd.select(n_dd.field).toPandas()['field']
    return df.select(["id"] + [col for col in n_cols]), f"{SILVER_DATA_PATH}.{table_name}"

def create_binary(spark, df, table_name = "binary"):
    """Create binary table. Use spark df functions instead of string builder."""
    b_dd = get_data_dictionary(spark, table_name)
    n_cols = b_dd.select(b_dd.field).toPandas()['field']
    b_df = build_binary_select(df, b_dd)
    return b_df, f"{SILVER_DATA_PATH}.{table_name}"

def join_dfs(*dfs, on='id', how='inner'):
    """Utility to join array of dataframes. Maybe move to util.
    
    Assert that we do not drop any rows during the course of the operation. 
    This assert is critical to force data alignment (everyone using same id snapshot).
    """
    l_df = dfs[0]
    start_nrows = l_df.count()
    for r_df in dfs[1:]: # NOTE: spark does not care that we overwrite l_df!
        l_df = l_df.join(r_df, on=on, how=how)
    finish_nrows = r_df.count()
    # check if anything was dropped. NOTE: maybe make optional since it throttles limit
    assert start_nrows == finish_nrows
    return l_df

def create_all_cardinality_dfs(spark, bronze_df):
    """Utility to create all the cardinality dataframes using the bronze dataframe."""
    sc_df, sc_name = create_smallcat(spark, bronze_df)
    n_df, n_name = create_number(spark, bronze_df)
    lc_df, lc_name = create_largecat(spark, bronze_df)
    b_df, b_name = create_binary(spark, bronze_df)
    # return as tuples. could be a class
    return (sc_df, sc_name), (n_df, n_name), (lc_df, lc_name), (b_df, b_name)

def run_cardinality(spark, write=False):
    df = spark.table(BRONZE_DATA_PATH)
    # construct the new silver dfs
    cdf_tuples = create_all_cardinality_dfs(spark, df)
    cdfs = [c_df for c_df, _ in cdf_tuples]
    # optionally write to db
    if write:
        for c_df, table_name in cdf_tuples: # TODO: partition?
            c_df.writeTo(table_name).createOrReplace()
    return cdfs

def run_join(spark, write=False):
    """Run all the functions for creating starter silver tables
    
    TODO: split up better so we can join without running everything.
    """
    # construct the new silver dfs
    sc_df = spark.table(f"{SILVER_DATA_PATH}.smallcat")
    lc_df = spark.table(f"{SILVER_DATA_PATH}.largecat")
    n_df = spark.table(f"{SILVER_DATA_PATH}.number")
    b_df = spark.table(f"{SILVER_DATA_PATH}.binary")                    
    # join them together
    census_df = join_dfs(sc_df, lc_df, n_df, b_df)
    census_name = f"{SILVER_DATA_PATH}.census"
    # optionally write to db
    if write:
        census_df.writeTo(census_name).createOrReplace()
    return census_df