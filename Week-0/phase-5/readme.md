# 📊 Phase 5 – Databricks + Olist (End-to-End Data Pipeline)

## 🔹 Project Overview

This project focuses on building a **complete end-to-end data pipeline** using **Databricks and PySpark** on the real-world **Olist e-commerce dataset**.

The pipeline covers the full lifecycle:

> **Data Ingestion → Validation → Transformation → Analytics → Reporting**

It demonstrates how raw data can be transformed into meaningful business insights using scalable data engineering practices.

---

## 🔹 Objective

* Design a robust and scalable data pipeline in Databricks
* Process and integrate multiple datasets into a unified model
* Perform analytical transformations and generate insights
* Build reporting-ready datasets for business use

---

## 🔹 Dataset Description

The Olist dataset consists of multiple relational tables:

* `orders`
* `customers`
* `order_items`
* `payments`
* `products`

These tables represent different aspects of an e-commerce platform and are integrated to create a centralized analytical model.

---

## 🔹 Architecture / Pipeline Flow

```
Data Ingestion → Data Validation → Data Transformation → Analytics → Reporting
```

---

## 🔹 Implementation Details

### ⚙️ Environment Setup

* Configured Databricks cluster
* Created modular notebooks for each pipeline stage
* Organized workflow for scalability and reusability

---

### 📥 Data Ingestion

* Uploaded CSV files into **Databricks FileStore**
* Structured data under schema: `olist`
* Loaded datasets into PySpark DataFrames

---

### ✅ Data Validation

* Performed schema validation
* Checked for null and missing values
* Verified data consistency
* Conducted sample data inspection

---

### 🧩 Data Modeling

* Designed a centralized **fact table (`fact_orders`)**
* Integrated dimension tables using appropriate join keys
* Ensured relational integrity across datasets

---

### 🔄 Transformation Layer

Applied key transformations:

| Transformation   | Purpose                                      |
| ---------------- | -------------------------------------------- |
| `join()`         | Combine multiple datasets                    |
| `groupBy()`      | Aggregate data                               |
| `agg()`          | Compute metrics (SUM, COUNT, etc.)           |
| `withColumn()`   | Create derived columns                       |
| `when()`         | Apply conditional logic                      |
| Window Functions | Advanced analytics (ranking, running totals) |

---

### 📈 Analytics Layer

Implemented advanced analytics using PySpark:

* **Top Customers per City** → Ranking functions
* **Daily & Cumulative Sales** → Running totals
* **Top Products by Category** → `DENSE_RANK()`
* **Customer Lifetime Value (CLV)** → Total spend per customer
* **Customer Segmentation**:

  * Gold
  * Silver
  * Bronze

---

### 📊 Reporting Layer

* Created final curated datasets
* Built consolidated reporting tables
* Prepared data for dashboards and visualization tools

---

## 🔹 Key Outputs / Deliverables

* Customer-level spend & segmentation dataset
* Product-level sales performance insights
* Daily and cumulative sales trends
* Final aggregated reporting table

---

## 🔹 Best Practices Followed

* Used correct join conditions to maintain data integrity
* Avoided duplicate records during joins
* Applied validation checks (row counts, sampling)
* Used appropriate data types for calculations
* Built modular and scalable pipeline design

---

## 🔹 Challenges & Solutions

| Challenge                                   | Resolution                                     |
| ------------------------------------------- | ---------------------------------------------- |
| Complex table relationships                 | Defined proper join keys after schema analysis |
| Missing attributes (e.g., product category) | Applied fallback logic and null handling       |
| File path issues in Databricks              | Standardized FileStore paths                   |
| Writing window functions                    | Tested incrementally with smaller datasets     |

---

## 🔹 Key Learnings

* Hands-on experience with real-world data pipelines
* Importance of fact table design in analytics
* Practical usage of window functions
* End-to-end workflow in Databricks with PySpark
* Translating business requirements into data transformations

---

## 🔹 Tech Stack

* **Platform:** Databricks
* **Language:** PySpark
* **Data Format:** CSV
* **Concepts:**

  * ETL (Extract, Transform, Load)
  * Data Modeling
  * Aggregations
  * Window Functions

---

## 🚀 Conclusion

This project demonstrates how to build a **production-style data pipeline** that transforms raw e-commerce data into **actionable business insights**, following industry-standard data engineering practices.
