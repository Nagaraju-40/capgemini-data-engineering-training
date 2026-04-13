# 🚗 Advanced Mini Pipeline – Car Sales (SQL + PySpark)

## 📌 Project Overview

This project builds a **realistic data engineering pipeline** using **PySpark and SQL** to process and analyze car sales data.
The pipeline handles data cleaning, validation, transformation, and advanced analytics including **dealer-level insights**.

---

## 🎯 Objective

* Work with large datasets using PySpark
* Perform data cleaning and validation
* Generate business insights using SQL
* Build an end-to-end data pipeline

---

## 🗂️ Dataset Description

The project uses the following tables:

1. **customers**

   * customer_id
   * name
   * city

2. **cars**

   * car_id
   * brand
   * model
   * price

3. **sales**

   * sale_id
   * customer_id
   * car_id
   * sale_date
   * quantity

4. **dealers**

   * dealer_id
   * name
   * city

5. **sales_dealer**

   * sale_id
   * dealer_id

---

## ⚙️ Pipeline Phases

### 🔹 Phase 1 – Data Understanding

* Load datasets into PySpark DataFrames
* Check schema and record counts
* Identify:

  * Null values
  * Duplicate records
  * Invalid entries

---

### 🔹 Phase 2 – Data Cleaning

* Handle missing values
* Fix negative prices
* Trim unwanted spaces in strings
* Remove invalid foreign key records

---

### 🔹 Phase 3 – Data Validation

* Identify invalid foreign keys using **left anti join**
* Generate a validation report

---

### 🔹 Phase 4 – Data Transformation

* Calculate **customer revenue**
* Perform **brand-wise sales analysis**
* Generate **city-wise revenue insights**

---

### 🔹 Phase 5 – Dealer Analytics

* Revenue per dealer
* Top dealers by revenue
* Dealer contribution by city

---

### 🔹 Phase 6 – SQL Analysis

* Top 3 customers per city
* Monthly sales trends
* Repeat customers analysis

---

### 🔹 Phase 7 – Output

* Save final transformed datasets
* Export analytics results
* Document pipeline with README

---

## 🛠️ Technologies Used

* Python
* PySpark
* SQL
* Databricks (optional)

---

## 📊 Key Features

* End-to-end data pipeline
* Real-world data cleaning scenarios
* Advanced joins and aggregations
* Business-level analytics


