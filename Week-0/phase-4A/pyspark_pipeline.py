# =========================================
# CUSTOMER SEGMENTATION PIPELINE (HUMANIZED)
# =========================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import Bucketizer
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank


# -----------------------------------------
# 1. Start Spark Session
# -----------------------------------------
spark = SparkSession.builder \
    .appName("Customer Segmentation Project") \
    .getOrCreate()


# -----------------------------------------
# 2. Load Customer and Sales Data
# -----------------------------------------
customers = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/samples/customers.csv")

sales = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/samples/sales.csv")


# -----------------------------------------
# 3. Clean the Data
# -----------------------------------------
# Remove null and duplicate customers
customers = customers \
    .dropna(subset=["customer_id"]) \
    .dropDuplicates(["customer_id"])

# Remove invalid sales data
sales = sales \
    .dropna(subset=["customer_id"]) \
    .dropDuplicates(["sale_id"]) \
    .filter(F.col("total_amount") > 0)


# -----------------------------------------
# 4. Calculate Total Spend per Customer
# -----------------------------------------
customer_spend = sales.groupBy("customer_id") \
    .agg(F.sum("total_amount").alias("total_spend"))

# Join with customer details
df = customer_spend.join(customers, "customer_id")

# Create full customer name
df = df.withColumn(
    "customer_name",
    F.concat_ws(" ", "first_name", "last_name")
)

print("=== Base Data ===")
df.select("customer_name", "total_spend").show()


# =========================================
# TASK 1: Segmentation using Conditions
# =========================================
# Business rule based segmentation

df_conditional = df.withColumn(
    "segment",
    F.when(F.col("total_spend") > 10000, "Gold")
     .when((F.col("total_spend") >= 5000) & (F.col("total_spend") <= 10000), "Silver")
     .otherwise("Bronze")
)

print("=== Conditional Segmentation ===")
df_conditional.select("customer_name", "total_spend", "segment").show()


# =========================================
# TASK 2: Count Customers per Segment
# =========================================
segment_count = df_conditional.groupBy("segment") \
    .agg(F.count("customer_id").alias("customer_count"))

print("=== Segment Distribution ===")
segment_count.show()


# =========================================
# TASK 3: Quantile-Based Segmentation
# =========================================
# Divide customers based on data distribution

quantiles = df.approxQuantile("total_spend", [0.33, 0.66], 0)
q1, q2 = quantiles

df_quantile = df.withColumn(
    "segment_quantile",
    F.when(F.col("total_spend") <= q1, "Bronze")
     .when((F.col("total_spend") <= q2), "Silver")
     .otherwise("Gold")
)

print("=== Quantile Segmentation ===")
df_quantile.select("customer_name", "total_spend", "segment_quantile").show()


# =========================================
# TASK 4: Bucketizer Segmentation
# =========================================
# Define spending ranges

splits = [-float("inf"), 5000, 10000, float("inf")]

bucketizer = Bucketizer(
    splits=splits,
    inputCol="total_spend",
    outputCol="bucket"
)

df_bucket = bucketizer.transform(df)

# Convert bucket numbers to labels
df_bucket = df_bucket.withColumn(
    "segment_bucket",
    F.when(F.col("bucket") == 0, "Bronze")
     .when(F.col("bucket") == 1, "Silver")
     .otherwise("Gold")
)

print("=== Bucketizer Segmentation ===")
df_bucket.select("customer_name", "total_spend", "segment_bucket").show()


# =========================================
# TASK 5: Window-Based Segmentation
# =========================================
# Rank customers based on spending

window = Window.orderBy("total_spend")

df_window = df.withColumn(
    "rank_pct",
    percent_rank().over(window)
)

df_window = df_window.withColumn(
    "segment_rank",
    F.when(F.col("rank_pct") <= 0.33, "Bronze")
     .when(F.col("rank_pct") <= 0.66, "Silver")
     .otherwise("Gold")
)

print("=== Window-based Segmentation ===")
df_window.select("customer_name", "total_spend", "rank_pct", "segment_rank").show()


# =========================================
# TASK 6: Compare All Segmentation Methods
# =========================================
comparison = df_conditional \
    .join(df_quantile.select("customer_id", "segment_quantile"), "customer_id") \
    .join(df_bucket.select("customer_id", "segment_bucket"), "customer_id") \
    .join(df_window.select("customer_id", "segment_rank"), "customer_id")

print("=== Comparison of All Methods ===")
comparison.select(
    "customer_name",
    "total_spend",
    "segment",            # Conditional
    "segment_quantile",   # Quantile
    "segment_bucket",     # Bucketizer
    "segment_rank"        # Window
).show()


# -----------------------------------------
# Stop Spark
# -----------------------------------------
spark.stop()