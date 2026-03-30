import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Retail_DQ_Test") \
    .getOrCreate()

CUSTOMERS_PATH = "sample_data/customers_sample.csv"
TRANSACTIONS_PATH = "sample_data/transactions_sample.parquet"


# -------------------------------
# TEST 1: Customers - No Nulls
# -------------------------------
def test_customers_no_nulls():
    df = spark.read.csv(CUSTOMERS_PATH, header=True, inferSchema=True)

    null_count = df.filter(
        col("customer_id").isNull() | col("country").isNull()
    ).count()

    assert null_count == 0, f"Null values found: {null_count}"


# -------------------------------
# TEST 2: Unique Customer IDs
# -------------------------------
def test_unique_customer_ids():
    df = spark.read.csv(CUSTOMERS_PATH, header=True, inferSchema=True)

    total = df.count()
    unique = df.select("customer_id").distinct().count()

    assert total == unique, "Duplicate customer_id found"


# -------------------------------
# TEST 3: Transactions > 0
# -------------------------------
def test_positive_order_value():
    df = spark.read.parquet(TRANSACTIONS_PATH)

    bad = df.filter(col("order_value") <= 0).count()

    assert bad == 0, f"Invalid order_value rows: {bad}"


# -------------------------------
# TEST 4: Valid Customer Mapping
# -------------------------------
def test_transaction_customer_mapping():
    cust_df = spark.read.csv(CUSTOMERS_PATH, header=True, inferSchema=True)
    trans_df = spark.read.parquet(TRANSACTIONS_PATH)

    invalid = trans_df.join(
        cust_df,
        "customer_id",
        "left_anti"
    ).count()

    assert invalid == 0, f"Invalid customer_id in transactions: {invalid}"


# -------------------------------
# TEST 5: Fraud Flag Valid
# -------------------------------
def test_fraud_flag_values():
    df = spark.read.parquet(TRANSACTIONS_PATH)

    invalid = df.filter(~col("fraud_label").isin(0,1)).count()

    assert invalid == 0, "Fraud flag must be 0 or 1"
