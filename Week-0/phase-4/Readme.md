#  Phase 4 – Mini Project: Business Pipeline & Analytics using PySpark

## objective

The objective of this phase is to build a complete end-to-end data pipeline using PySpark.

This phase focuses on transitioning from isolated transformations to a structured ETL workflow that generates meaningful business insights from raw data.


## Problem Summary

In this project, we aim to build a **complete business data pipeline** using PySpark that processes customer and sales data to generate meaningful business insights.

The main challenges include:

* Handling **raw CSV data**
* Cleaning inconsistent or duplicate records
* Performing **data transformations**
* Generating **aggregated insights**
* Producing a **final analytical report**

---

## Key Challenges

*  Missing or null values in customer and sales data
*  Duplicate records affecting accuracy
*  Incorrect or negative transaction values
*  Combining multiple datasets efficiently (JOIN operations)
*  Deriving meaningful business insights from raw data

---

## Approach


1. **Data Ingestion**

   * Loaded CSV data using PySpark DataFrame API

2. **Data Cleaning**

   * Removed null values
   * Eliminated duplicates
   * Filtered invalid transactions

3. **Data Transformation**

   * Created new columns (e.g., customer name)
   * Joined datasets for enriched insights

4. **Aggregation & Analysis**

   * Computed sales metrics
   * Identified top customers
   * Segmented users

5. **Final Reporting**

   * Generated a consolidated report
   * Saved output as CSV

---

## Key Transformations Used

### Data Cleaning

* `dropna()` → Removed null records
* `dropDuplicates()` → Removed duplicate entries
* `filter()` → Removed invalid sales (amount > 0)

### Data Joining

* `join()` → Combined customer and sales datasets

### Aggregations

* `groupBy()` + `sum()` → Total sales/revenue
* `count()` → Number of orders

### Column Transformations

* `withColumn()` → Created new columns
* `concat_ws()` → Combined first & last names

### Conditional Logic

* `when()` → Customer segmentation (Gold, Silver, Bronze)

### Sorting & Ranking

* `orderBy()` → Top customers identification


## Learnings

*  Hands-on experience with **PySpark DataFrames**
*  Understanding of **ETL pipeline design**
*  Real-world use of **data cleaning techniques**
*  Performing **joins and aggregations at scale**
*  Building **business-driven insights**


## Output

The pipeline generates the following outputs:

1. Daily Sales
2. City-wise Revenue
3. Top 5 Customers
4. Repeat Customers
5. Final Report Table





