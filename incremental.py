
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re, logging, os
from logging.handlers import RotatingFileHandler

# -----------------------------------
# LOGGING
# -----------------------------------
log_dir = "/tmp/praveen_proj/log"
os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger("incremental_pipeline")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = RotatingFileHandler(f"{log_dir}/incremental.log", maxBytes=5*1024*1024, backupCount=3)
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    console.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addHandler(console)

# -----------------------------------
# SPARK
# -----------------------------------
spark = SparkSession.builder \
    .appName("Retail_Incremental") \
    .enableHiveSupport() \
    .getOrCreate()

silver_path = "/tmp/praveen_proj/silver/"

# -----------------------------------
# CLEAN FUNCTION
# -----------------------------------
def clean(df):
    df = df.dropna().dropDuplicates()
    for c in df.columns:
        new_c = re.sub(r'[^\w\s]', '', c.lower())
        new_c = re.sub(r'\s', '_', new_c)
        df = df.withColumnRenamed(c, new_c)
    return df

# -----------------------------------
# INCREMENTAL CUSTOMERS
# -----------------------------------
def incremental_customers():
    logger.info("Starting Incremental Load for Customers")

    new_df = spark.read.csv(
        "file:///home/ec2-user/250226batch/praveen/customers2.csv",
        header=True, inferSchema=True
    )

    old_df = spark.read.parquet(silver_path + "customers")
    last_id = old_df.select(max("customer_id")).collect()[0][0]

    df = new_df.filter(col("customer_id") > last_id)
    df = clean(df)

    df.write.mode("append").partitionBy("country").parquet(silver_path + "customers")
    logger.info("Incremental load completed")

# -----------------------------------
# GOLD → HIVE
# -----------------------------------
def generate_gold():
    logger.info("Refreshing Gold to Hive")

    t = spark.read.parquet(silver_path + "transactions")
    c = spark.read.parquet(silver_path + "customers")
    p = spark.read.parquet(silver_path + "products")
    b = spark.read.parquet(silver_path + "behavior")

    t.createOrReplaceTempView("v_transactions")
    c.createOrReplaceTempView("v_customers")
    p.createOrReplaceTempView("v_products")
    b.createOrReplaceTempView("v_behavior")

    spark.sql("CREATE DATABASE IF NOT EXISTS retail_hive_db")

    # Example Gold tables
    spark.sql("""
        SELECT SUM(order_value) total_revenue FROM v_transactions
    """).write.mode("overwrite").saveAsTable("retail_hive_db.revenue_summary")

    spark.sql("""
        SELECT p.category, SUM(t.order_value) total_revenue
        FROM v_transactions t JOIN v_products p ON t.product_id=p.product_id
        GROUP BY p.category
    """).write.mode("overwrite").saveAsTable("retail_hive_db.revenue_by_category")

    spark.sql("""
        SELECT COUNT(*) total_customers FROM v_customers
    """).write.mode("overwrite").saveAsTable("retail_hive_db.total_customers")

    logger.info("Gold layer refreshed in Hive successfully")

# -----------------------------------
# MAIN
# -----------------------------------
if __name__ == "__main__":
    incremental_customers()
    generate_gold()
