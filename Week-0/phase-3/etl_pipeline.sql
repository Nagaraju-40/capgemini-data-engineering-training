# =========================================
# ETL PIPELINE USING SPARK SQL
# =========================================

from pyspark.sql import SparkSession

# 1. EXTRACT
spark = SparkSession.builder.appName("Customers-Sales ETL SQL").getOrCreate()

customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/samples/customers.csv")

sales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/samples/sales.csv")

# Create temp views
customers.createOrReplaceTempView("customers")
sales.createOrReplaceTempView("sales")


# =========================================
# 2. TRANSFORM (CLEANING USING SQL)
# =========================================

# Clean sales data
sales_clean = spark.sql("""
    SELECT *
    FROM sales
    WHERE customer_id IS NOT NULL
      AND quantity IS NOT NULL
      AND total_amount IS NOT NULL
      AND quantity > 0
      AND total_amount > 0
""")

# Clean customers data
customers_clean = spark.sql("""
    SELECT *
    FROM customers
    WHERE customer_id IS NOT NULL
""")

# Create temp views
sales_clean.createOrReplaceTempView("sales_clean")
customers_clean.createOrReplaceTempView("customers_clean")


# =========================================
# 3. JOIN
# =========================================
spark.sql("""
    CREATE OR REPLACE TEMP VIEW joined AS
    SELECT s.*, c.city
    FROM sales_clean s
    JOIN customers_clean c
    ON s.customer_id = c.customer_id
""")


# =========================================
# 4. BUSINESS LOGIC
# =========================================

# 4.1 Daily Sales
daily_sales = spark.sql("""
    SELECT sale_date,
           SUM(total_amount) AS daily_sales
    FROM sales_clean
    GROUP BY sale_date
""")

# 4.2 City-wise Revenue
city_revenue = spark.sql("""
    SELECT city,
           SUM(total_amount) AS revenue
    FROM joined
    GROUP BY city
""")

# 4.3 Repeat Customers (>2 orders)
repeat_customers = spark.sql("""
    SELECT customer_id,
           COUNT(*) AS order_count
    FROM sales_clean
    GROUP BY customer_id
    HAVING COUNT(*) > 2
""")

# 4.4 Top Customer per City (Window Function)
top_customers = spark.sql("""
    SELECT *
    FROM (
        SELECT city,
               customer_id,
               SUM(total_amount) AS total_spent,
               ROW_NUMBER() OVER (PARTITION BY city ORDER BY SUM(total_amount) DESC) AS rank
        FROM joined
        GROUP BY city, customer_id
    ) t
    WHERE rank = 1
""")

# 4.5 Final Report
final_report = spark.sql("""
    SELECT customer_id,
           city,
           SUM(total_amount) AS total_spend,
           COUNT(*) AS order_count
    FROM joined
    GROUP BY customer_id, city
""")


# =========================================
# 5. OUTPUT
# =========================================

print("Daily Sales:")
daily_sales.show()

print("City Revenue:")
city_revenue.show()

print("Repeat Customers:")
repeat_customers.show()

print("Top Customers per City:")
top_customers.show()

print("Final Report:")
final_report.show()


# =========================================
# 6. LOAD
# =========================================
final_report.write.mode("overwrite").parquet("/tmp/final_report_sql")


# =========================================
# 7. STOP
# =========================================
spark.stop()