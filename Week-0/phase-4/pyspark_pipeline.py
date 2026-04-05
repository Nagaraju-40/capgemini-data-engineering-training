# =========================================
# PHASE 4 - FINAL BUSINESS PIPELINE
# =========================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as Fn

# -----------------------------------------
# 1. Initialize Spark
# -----------------------------------------
spark = SparkSession.builder \
    .appName("Phase4_Final_Pipeline") \
    .getOrCreate()

# -----------------------------------------
# 2. Load Data
# -----------------------------------------
customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/samples/customers.csv")

sales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/samples/sales.csv")

# -----------------------------------------
# 3. Data Cleaning
# -----------------------------------------
customers = customers.dropna(subset=["customer_id"]) \
                     .dropDuplicates(["customer_id"])

sales = sales.dropna(subset=["customer_id"]) \
             .dropDuplicates(["sale_id"]) \
             .filter(Fn.col("total_amount") > 0)

# -----------------------------------------
# 4. Create Customer Name
# -----------------------------------------
customers = customers.withColumn(
    "customer_name",
    Fn.concat_ws(" ", Fn.col("first_name"), Fn.col("last_name"))
)

# -----------------------------------------
# 5. JOIN (Reuse)
# -----------------------------------------
joined = sales.join(customers, "customer_id")

# =========================================
# TASK 1: Daily Sales
# =========================================
daily_sales = sales.groupBy("sale_date") \
    .agg(Fn.sum("total_amount").alias("total_sales"))

# =========================================
# TASK 2: City-wise Revenue
# =========================================
city_revenue = joined.groupBy("city") \
    .agg(Fn.sum("total_amount").alias("total_revenue"))

# =========================================
# TASK 3: Top 5 Customers
# =========================================
top_customers = joined.groupBy("customer_name") \
    .agg(Fn.sum("total_amount").alias("total_spend")) \
    .orderBy(Fn.desc("total_spend")) \
    .limit(5)

# =========================================
# TASK 4: Repeat Customers (>1 order)
# =========================================
repeat_customers = sales.groupBy("customer_id") \
    .agg(Fn.count("sale_id").alias("order_count")) \
    .filter(Fn.col("order_count") > 1)

# =========================================
# TASK 5: Customer Segmentation
# =========================================
customer_spend = sales.groupBy("customer_id") \
    .agg(Fn.sum("total_amount").alias("total_spend"))

segmentation = customer_spend.join(customers, "customer_id") \
    .withColumn(
        "segment",
        Fn.when(Fn.col("total_spend") > 10000, "Gold")
         .when((Fn.col("total_spend") >= 5000) & (Fn.col("total_spend") <= 10000), "Silver")
         .otherwise("Bronze")
    )

# =========================================
# TASK 6: Final Reporting Table
# =========================================
final_df = segmentation.join(repeat_customers, "customer_id", "left") \
    .fillna({"order_count": 1}) \
    .select("customer_name", "city", "total_spend", "order_count", "segment")

# =========================================
# TASK 7: Save Output (IMPORTANT FIX)
# =========================================
final_df.coalesce(1).write \
    .mode("overwrite") \
    .csv("/tmp/report", header=True)

# =========================================
# DISPLAY OUTPUTS
# =========================================
print("----- DAILY SALES -----")
daily_sales.show()

print("----- CITY REVENUE -----")
city_revenue.show()

print("----- TOP 5 CUSTOMERS -----")
top_customers.show()

print("----- REPEAT CUSTOMERS -----")
repeat_customers.show()

print("----- FINAL REPORT -----")
final_df.show()

# =========================================
# STOP SPARK
# =========================================
spark.stop()