# 📊 Phase 4A: Customer Segmentation Pipeline (PySpark)


## 🎯 Objective

Work with a real-world multi-table dataset in Databricks. Perform ingestion, validation, modeling,advanced analytics using window functions, and build a complete reporting pipeline.


## 🔍 Problem Summary

Businesses often have large volumes of customer transaction data but lack clear insights into:

* Who are the **top customers**
* How customers can be **grouped based on spending**
* Which segmentation method is **more effective**

The challenge is to:

* Clean and process raw data
* Calculate total spending per customer
* Apply multiple segmentation techniques
* Compare results for better decision-making

---

## Approach
### 1️⃣ Data Loading

* Load customer and sales data from CSV files into PySpark DataFrames

### 2️⃣ Data Cleaning

* Remove null values
* Remove duplicate records
* Filter invalid transactions (amount > 0)

### 3️⃣ Feature Engineering

* Calculate **total_spend per customer**
* Create **customer_name** column

### 4️⃣ Apply Segmentation Techniques

Implemented 4 different segmentation methods:

* Conditional Logic (fixed thresholds)
* Quantile-based segmentation
* Bucketizer (range-based binning)
* Window-based ranking (percentile ranking)
---

## 🔧 Key Transformations

### 🧹 Data Cleaning

* `dropna()` → Remove null values
* `dropDuplicates()` → Remove duplicate records
* `filter()` → Remove invalid sales

---

###  Aggregation
---

###  Column Creation

### Conditional Segmentation

###  Quantile Segmentation

###  Bucketizer


## 📈 Output

### 1️⃣ Conditional Segmentation

* Customers categorized using fixed thresholds

### 2️⃣ Segment Distribution

* Count of customers in each segment

### 3️⃣ Quantile Segmentation

* Balanced segmentation based on data distribution

### 4️⃣ Bucketizer Segmentation

* Rule-based segmentation using ranges

### 5️⃣ Window-based Segmentation

* Ranking-based segmentation using percentiles


## 📚 Learnings

* ✅ Understanding of **customer segmentation techniques**
* ✅ Hands-on with **PySpark transformations and actions**
* ✅ Use of **approxQuantile for data-driven insights**
* ✅ Implementation of **Bucketizer for binning**
* ✅ Knowledge of **window functions and ranking**
* ✅ Comparing multiple approaches for better decision-making
* ✅ Real-world experience in building a **data analytics pipeline**

---

