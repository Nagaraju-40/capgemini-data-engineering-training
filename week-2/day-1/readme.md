# 📊 Insurance Data Engineering Pipeline

## 📌 Project Overview

This project focuses on building a **data engineering pipeline** for an insurance domain. The goal is to process raw data, clean and validate it, and generate meaningful insights related to **premium collection, claims, risk, and agent performance**.

The pipeline follows a structured approach using PySpark and SQL to ensure data quality and analytical accuracy.

---

## 🏢 Domain Understanding

In the insurance ecosystem:

* Customers purchase policies and pay premiums
* Policies may generate claims
* Agents sell and manage policies
* Business objective:

  * Maximize **premium revenue**
  * Minimize **risk (claims)**

---

## 📂 Dataset Description

The project uses the following datasets:

* **customers** → Customer information
* **policies** → Policies purchased by customers
* **claims** → Claims raised against policies
* **agents** → Agent details
* **policy_agent** → Mapping between policies and agents

---

## ⚙️ Pipeline Phases

### 🔹 Phase 1: Data Understanding

* Loaded datasets using PySpark
* Checked schema using `printSchema()`
* Verified row counts
* Identified issues:

  * Null values
  * Negative values
  * Invalid foreign keys
* Understood relationships:

  * Customer → Policy → Claim → Agent

---

### 🔹 Phase 2: Data Cleaning

* Handled null values (filled/dropped based on context)
* Removed or corrected:

  * Negative premium values
  * Negative claim amounts
* Standardized string columns (trim, uppercase)
* Corrected data types (dates, numeric fields)

---

### 🔹 Phase 3: Data Validation

* Used **left anti joins** to detect invalid records:

  * Policies without customers
  * Claims without policies
* Created validation report:

  * Total records
  * Invalid records
  * Cleaned records
* Verified row counts before and after joins

---

### 🔹 Phase 4: Data Transformation

* Joined datasets carefully to avoid duplication
* Created derived metrics:

  * Total premium per customer
  * Total claim per customer
  * Risk Score = Total Claim / Total Premium
* Computed city-level insights:

  * Premium distribution
  * Claim distribution

---

### 🔹 Phase 5: Advanced SQL (CTE)

* Created temporary views from DataFrames
* Used SQL **WITH (CTE)** for step-wise logic:

  * Aggregate customer premium
  * Aggregate customer claims
  * Compute risk score
* Key outputs:

  * Top 3 risky customers per city
  * Customers with increasing claims month-over-month

---

### 🔹 Phase 6: Window Functions

* Applied:

  * `ROW_NUMBER()`
  * `RANK()`
  * `DENSE_RANK()`
* Use cases:

  * Rank agents based on total premium
  * Rank customers based on risk score
* Used proper partitioning (e.g., by city)

---

### 🔹 Phase 7: Final Output

* Generated final datasets
* Saved outputs to storage
* Ensured correctness through validation checks

---

## 🔍 Data Quality Issues Identified

* Missing values in key columns
* Negative premium and claim amounts
* Invalid foreign key relationships
* Data inconsistency in string fields

---

## 🛠️ Data Cleaning Approach

* Null handling using fill/drop strategies
* Data standardization (trim, uppercase)
* Type casting for accuracy
* Business-rule-based corrections

---

## ✅ Validation Strategy

* Referential integrity checks using joins
* Record count reconciliation
* Cross-validation of aggregates
* Step-by-step pipeline validation

---

## 📈 Key Insights

* High-risk customers identified using risk score
* Cities with higher claim ratios detected
* Agent performance ranked based on premium contribution
* Claim trends analyzed over time

---

## ⚠️ Important Learnings

* Never trust output without validation
* Joins can introduce duplication if not handled carefully
* Always build pipelines step-by-step
* Data quality directly impacts business insights

---

## 🧰 Technologies Used

* PySpark
* Spark SQL
* DataFrame API
* Window Functions

---

