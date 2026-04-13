# ⚡ Phase 6 – Spark Playground Exit Sprint (Advanced Practice Lab)

## 🔹 Project Overview

This project focuses on solving **advanced real-world data engineering challenges** using **PySpark**.

It simulates working with **imperfect datasets**, emphasizing:

* Data cleaning
* Data validation
* Advanced transformations
* Analytical processing

The pipeline is designed to reflect a **production-style ETL workflow**.

---

## 🔹 Objective

* Strengthen PySpark proficiency through hands-on problem solving
* Handle real-world dirty datasets effectively
* Apply advanced joins and window functions
* Build a complete, end-to-end data pipeline

---

## 🔹 Problem Statement

The datasets (`customers`, `orders`) contained multiple data quality issues:

* Null values in critical fields
* Invalid records (e.g., negative transaction amounts, incorrect foreign keys)
* Duplicate entries

---

## 🔹 Architecture / Pipeline Flow

```id="l55h57"
Data Ingestion → Data Cleaning → Data Validation → Data Transformation → Analytics → Reporting
```

---

## 🔹 Implementation Details

### 📥 Data Ingestion

* Loaded raw CSV datasets into PySpark DataFrames
* Ensured schema consistency during ingestion

---

### 🧹 Data Cleaning

* Removed null values using `dropna()`
* Filtered invalid records (e.g., negative amounts)
* Standardized string columns using trimming functions
* Eliminated duplicates using `dropDuplicates()`

---

### ✅ Data Validation

* Ensured referential integrity between datasets
* Used **`left_anti` join** to detect invalid foreign key relationships
* Verified data consistency before transformations

---

### 🔗 Data Integration

Applied appropriate join strategies:

* **Inner Join** → Retained only valid matching records
* **Left Join** → Used for validation and completeness checks

---

### 🔄 Transformation Layer

* Aggregated customer-level metrics:

  * Total spend
  * Order count
* Applied filters to maintain clean and reliable datasets

---

### 📈 Analytics Layer

Implemented advanced analytics using window functions:

* Customer ranking based on total spend
* Running totals for cumulative analysis
* Lag-based comparisons for trend insights

---

### 📅 Time-Based Analysis

* Extracted date features using `to_date()` and `month()`
* Performed **monthly sales aggregation**
* Analyzed trends over time

---

### 📊 Output Generation

* Generated curated datasets for reporting
* Stored cleaned and transformed outputs for downstream use

---

## 🔹 Key Transformations

| Transformation         | Purpose                                  |
| ---------------------- | ---------------------------------------- |
| `join()`               | Combine customers and orders             |
| `left_anti` join       | Detect invalid foreign key relationships |
| `groupBy()`            | Perform aggregations                     |
| `agg()`                | Compute metrics (SUM, COUNT)             |
| `filter()`             | Remove invalid records                   |
| `dropna()`             | Handle missing values                    |
| `dropDuplicates()`     | Remove duplicate records                 |
| Window Functions       | Ranking, running totals, lag analysis    |
| `to_date()`, `month()` | Date transformation & feature extraction |

---

## 🔹 Analytical Use Cases

### 🔍 Data Quality Validation

* Identified invalid foreign key relationships using `left_anti` join

### 👤 Customer-Level Metrics

* Total spend per customer
* Order count per customer

### 🏆 Customer Ranking

* Ranked customers based on total spending using window functions

### 📊 Time-Series Analysis

* Monthly sales aggregation
* Trend analysis over time

---

## 🔹 Outputs / Deliverables

* Cleaned and standardized datasets (`customers`, `orders`)
* Invalid records report (foreign key mismatches)
* Aggregated customer-level metrics
* Ranked customer dataset
* Monthly sales summary dataset

---

## 🔹 Best Practices Followed

* Performed data cleaning before transformations
* Ensured referential integrity using joins
* Prevented duplicates after joins
* Handled null and invalid values systematically
* Validated outputs using row counts and sampling

---

## 🔹 Challenges & Solutions

| Challenge                        | Resolution                                    |
| -------------------------------- | --------------------------------------------- |
| Identifying invalid foreign keys | Used `left_anti` joins for accurate detection |
| Handling dirty data              | Applied systematic cleaning and filtering     |
| Understanding window functions   | Practiced incrementally on smaller datasets   |
| Managing complex transformations | Broke pipeline into modular steps             |

---

## 🔹 Key Learnings

* Mastered advanced join strategies (`inner`, `left`, `left_anti`)
* Understood importance of data validation in ETL pipelines
* Applied window functions for real-world analytics
* Gained experience handling messy datasets
* Designed scalable, end-to-end PySpark pipelines

---

## 🔹 Tech Stack

* **Platform:** Apache Spark / Databricks
* **Language:** PySpark
* **Data Format:** CSV
* **Concepts:**

  * ETL Pipeline Design
  * Data Cleaning & Validation
  * Aggregations
  * Window Functions

---

## 🚀 Conclusion

This project demonstrates the ability to work with **real-world imperfect data**, applying **advanced PySpark techniques** to build a clean, reliable, and analytics-ready data pipeline aligned with industry best practices.
