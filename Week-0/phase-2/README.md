# 📊 PySpark Customer Sales Analysis

## 🚀 Project Overview

This project focuses on analyzing customer transaction data using **PySpark**.
It covers multiple real-world data engineering tasks such as data cleaning, joining datasets, aggregation, and deriving business insights.

---

## 🎯 Objectives

* Calculate total order amount per customer
* Identify top customers based on spending
* Find customers with no orders
* Analyze city-wise revenue
* Compute average order value
* Identify repeat customers

---

## 🧠 What I Learned

* 🔹 How to work with **PySpark DataFrames**
* 🔹 Performing **joins between multiple datasets**
* 🔹 Importance of **data cleaning (trim, null handling)**
* 🔹 Using **aggregation functions** like `sum()`, `count()`, `avg()`
* 🔹 Understanding **groupBy operations**
* 🔹 Debugging **empty joins and schema issues**
* 🔹 Converting data types using `.cast()`

---

## 🛠️ What I Practiced

* ✔ Loading CSV data into PySpark
* ✔ Data transformation and preprocessing
* ✔ Writing multiple analytical queries
* ✔ Using:

  * `groupBy()`
  * `agg()`
  * `orderBy()`
  * `filter()`
* ✔ Performing **left joins and inner joins**
* ✔ Sorting and limiting results

---

## ⚠️ Challenges Faced

### 🔹 Missing `total_amount` Column

The dataset did not contain a direct amount column.
👉 Learned that real-world datasets often require **derived calculations**.

---

### 🔹 Join Issues (Empty Results)

Faced issues where joins returned empty outputs.
👉 Resolved by:

* Cleaning keys using `trim()`
* Ensuring consistent data types

---

### 🔹 Data Type Problems

Columns like numbers were loaded as strings.
👉 Fixed using `.cast()` to proper types.

---

## 📈 Outcomes

* ✅ Successfully calculated **customer-wise total spending**
* ✅ Identified **top 3 high-value customers**
* ✅ Found **customers with no orders**
* ✅ Generated **city-wise revenue insights**
* ✅ Computed **average order value per customer**
* ✅ Identified **repeat customers**
* ✅Sorting data based on ** aggregated values**

---


## ⚙️ Technologies Used

* 🐍 Python
* ⚡ PySpark
* 💻 VS Code / Spark Playground

---

