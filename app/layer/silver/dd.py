"""Ingest data dictionary from bronze into 4 tables."""

import metadata
import pyspark.sql.functions as pys_fn
import numpy as np

NAMESPACE = "silver.md"

def create_all(spark, write=False):
    dd = load_dd(spark)
    # create the df for each category.
    sc_dd, sc_name = create_smallcat(spark, dd)
    b_dd, b_name = create_binary(spark, dd)
    lc_dd, lc_name = create_largecat(spark, dd)
    n_dd, n_name = create_number(spark, dd)    
    # write out results
    if write:
        sc_dd.writeTo(sc_name).createOrReplace()
        b_dd.writeTo(b_name).createOrReplace()
        n_dd.writeTo(n_name).createOrReplace()
        lc_dd.writeTo(lc_name).createOrReplace()
    return dd, sc_dd, b_dd, lc_dd, n_dd # for debugging, can ignore

def load_dd(spark):
    """Function to load the data dictionary.
    
    Encapsulated for easy testing since all the functions need it.
    """
    return spark.table(f"{metadata.CATALOG}.bronze.dd")

def create_smallcat(spark, dd, table_name='smallcat'):
    """Use data dictionary to create metadata for small categorical features.
    
    Return the DF rather than creating it for composability. Same for the other creates.
    """
    md_cols = metadata.INPUT_LIST
    dd_sc = dd.filter(dd.cardinality == "small categorical")[dd.field.isin(md_cols)]
    dd_sc_expl = dd_sc.withColumn(
        "T", pys_fn.arrays_zip("codes", "texts")
    ).withColumn(
        "T", pys_fn.explode("T")
    ).select(
        "field", pys_fn.col("T.codes"), pys_fn.col("T.texts")
    ).sort("field")
    dd_sc_expl.createOrReplaceTempView("dd_sc_expl")
    dd_smallcat = spark.sql("""
    SELECT CONCAT(field, "_", COALESCE(code, 'NULL')) AS silver_column, field, CAST (code AS int), texts
    FROM (
        SELECT field, CASE codes WHEN 'null_str' THEN NULL ELSE codes END code, texts
        FROM dd_sc_expl )
        """
    )
    # using limit query instead of direct CTAS
    return dd_smallcat, f"{metadata.CATALOG}.{NAMESPACE}.{table_name}"

def create_binary(spark, dd, table_name='binary'):
    """Use data dictionary to create metadata for binary features."""
    md_cols = metadata.INPUT_LIST
    dd_b = dd.filter(dd.cardinality == 'binary')[dd.field.isin(md_cols)]
    # switch to pandas. TODO: maybe can write in cleaner way?
    cols = ["field", "codes", "has_null", "texts"]
    pdd_b = dd_b.select(cols).toPandas()
    null_idx = [ codes[0] == "null_str" for codes in pdd_b["codes"] ]
    code_true = np.array([ codes[0+isnull] for isnull, codes in zip(null_idx, pdd_b["codes"]) ])
    code_false = np.array([ 
        codes[1+isnull] if len(codes) > 1 else 'NULL' 
        for isnull, codes in zip(null_idx, pdd_b["codes"]) 
    ])
    # guard against error if true and false are marked as same for some reason
    code_false[code_true == code_false] = 0
    pdd_b["code_true"] = code_true
    pdd_b["code_false"] = code_false
    dd_binary = spark.createDataFrame(pdd_b)
    return dd_binary, f"{metadata.CATALOG}.{NAMESPACE}.{table_name}"

def create_largecat(spark, dd, table_name='largecat'):
    """Use data dictionary to create metadata for largecat features.
    
    TODO: Currently no large categories used.
    """
    md_cols = metadata.INPUT_LIST
    dd_lc = dd.filter(dd.cardinality == 'large categorical')[dd.field.isin(md_cols)].select("field", "codes", "texts", "name_text")
    return dd_lc, f"{metadata.CATALOG}.{NAMESPACE}.{table_name}"

def create_number(spark, dd, table_name='number'):
    """Use data dictionary to create metadata for numeric features."""
    md_cols = metadata.INPUT_LIST
    dd_n = dd.filter(dd.cardinality == 'number')[dd.field.isin(md_cols)].select("field", "codes", "texts", "name_text")
    return dd_n, f"{metadata.CATALOG}.{NAMESPACE}.{table_name}"
