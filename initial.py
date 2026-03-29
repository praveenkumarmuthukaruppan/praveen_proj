
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

logger = logging.getLogger("initial_pipeline")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = RotatingFileHandler(f"{log_dir}/initial.log", maxBytes=5*1024*1024, backupCount=3)
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
    .appName("Retail_Initial") \
    .enableHiveSupport() \
    .getOrCreate()

bronze_path = "/tmp/praveen_proj/bronze/"
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
# EXTRACT → BRONZE
# -----------------------------------
def load_table():
    logger.info("Loading source to Bronze")
    try:
        # Behavior and Products from PostgreSQL
        pg_url = "jdbc:postgresql://13.42.152.118:5432/testdb"
        pg_props = {"user":"admin","password":"admin123","driver":"org.postgresql.Driver"}

        spark.read.jdbc(pg_url, "praveen.behavior", properties=pg_props) \
            .write.mode("overwrite").parquet(bronze_path + "behavior")

        spark.read.jdbc(pg_url, "praveen.products", properties=pg_props) \
            .write.mode("overwrite").parquet(bronze_path + "products")

        # Customers from CSV
        spark.read.option("header", True).option("inferSchema", True) \
            .csv("file:///home/ec2-user/250226batch/praveen/customers1.csv") \
            .write.mode("overwrite").parquet(bronze_path + "customers")

        logger.info("Bronze layer loaded successfully")
    except Exception as e:
        logger.error(f"Error in load_table: {e}")
        raise

# -----------------------------------
# BRONZE → SILVER
# -----------------------------------
def transform():
    logger.info("Cleaning Bronze → Silver")

    tables = ["customers", "products", "behavior", "transactions"]
    for table in tables:
        path = bronze_path + table
        if table == "transactions":
            df = spark.read.parquet(path)
            df = clean(df)
            df.write.mode("overwrite").partitionBy("order_date").parquet(silver_path + table)
        elif table == "customers":
            df = spark.read.parquet(path)
            df = clean(df)
            df.write.mode("overwrite").partitionBy("country").parquet(silver_path + table)
        else:
            df = spark.read.parquet(path)
            df = clean(df)
            df.write.mode("overwrite").parquet(silver_path + table)

        logger.info(f"{table} cleaned and saved to Silver")

# -----------------------------------
# GOLD → HIVE
# -----------------------------------
def generate_gold():
    logger.info("Generating Gold to Hive")

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

    logger.info("Gold layer written to Hive successfully")

# -----------------------------------
# MAIN
# -----------------------------------
if __name__ == "__main__":
    logger.info("INITIAL PIPELINE STARTED")
    load_table()
    transform()
    generate_gold()
    logger.info("INITIAL PIPELINE COMPLETED")
