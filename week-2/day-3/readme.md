## 🎯 Objective (In Simple Words)

Build a data pipeline that takes **raw messy data → cleans it → turns it into useful business insights** using Medallion Architecture (Bronze, Silver, Gold).

---

## 🏗️ What is Medallion Architecture?

It’s a **3-step process** to improve data quality step by step:

* 🥉 **Bronze** → Raw data
* 🥈 **Silver** → Clean data
* 🥇 **Gold** → Business insights

---

## 🔄 Your Pipeline (Easy Explanation)

### 🥉 Bronze Layer (Raw Data)

* Loaded CSV data into PySpark
* Stored it in Delta Lake
* No changes made (just raw data)

👉 Think: *“Store data as it is”*

---

### 🥈 Silver Layer (Cleaned Data)

Here you fixed all problems:

* Filled or handled NULL values
* Converted data types (string → int/date)
* Removed duplicate records
* Kept only latest updated data
* Removed negative values

👉 Think: *“Clean and prepare the data properly”*

---

### 🥇 Gold Layer (Business Insights)

Here you created useful outputs:

* Sales by product, category, city
* Customer insights:

  * Total orders
  * Total spending
* Found top customers and products

👉 Think: *“Turn data into insights for business decisions”*

---

## 🧪 Extra Work You Did (Important)

You also created your own dataset:

* Generated **20,000+ records** using PySpark
* Added real-world issues:

  * NULL values
  * Duplicate records
  * Negative values
  * Incremental updates

👉 This made your project more realistic 👍

---

## ⚠️ Problems You Solved

* Missing data (NULLs)
* Duplicate entries
* Wrong values (negative amounts)
* Updating old records with new ones

---

## 🧠 Key Learnings (Simple)

* How to structure data using layers
* How to clean messy real-world data
* How to validate and trust your data
* How to build scalable pipelines

---

## 🛠️ Tools Used

* PySpark
* Delta Lake
* Python

---

## 🚀 Final Outcome

* Built a pipeline from **raw → clean → insights**
* Created **ready-to-use data for dashboards and analytics**