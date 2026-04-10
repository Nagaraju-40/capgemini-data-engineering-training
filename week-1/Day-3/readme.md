

# 📊 PySpark Transformations Project

### Case Functions | Window Functions | Date Functions

---

## 🎯 Objective

To demonstrate the use of **Case Functions, Window Functions, and Date Functions in PySpark** for transforming, analyzing, and deriving meaningful insights from structured data.

---

## 📌 Problem Summary

The dataset contains transactional and customer-related data where:

* Categorization logic is missing (e.g., sales levels)
* No ranking or ordering of records within groups
* Date fields are not fully utilized for time-based insights

To make the dataset more analytical, we need to:

* Create conditional columns
* Perform ranking and aggregation within partitions
* Extract meaningful information from date columns

---

## ⚠️ Key Challenges

* Writing conditional logic efficiently in PySpark
* Applying window functions without impacting performance
* Understanding partitioning and ordering in window operations
* Handling date formats and extracting components correctly
* Combining multiple transformations in a clean pipeline

---

## 🛠️ Approach

1. Load dataset into PySpark DataFrame
2. Explore schema and data
3. Apply transformations in stages:

   * Use **case functions** for conditional columns
   * Use **window functions** for ranking and aggregations
   * Use **date functions** for extracting time-based insights
4. Validate results and ensure correctness

---

## 🔄 Key Transformations Used

### ✅ Case Functions

### ✅ Window Functions

### ✅ Date Functions

## 📚 Learnings

* How to implement conditional logic using `when()`
* Understanding window partitioning and ranking functions
* Extracting insights from date columns
* Combining multiple transformations efficiently
* Writing clean and scalable PySpark code

---

## 📊 Output

After applying transformations:

* New categorical column (`Sales_Category`) created
* Ranking of customers within each country
* Extracted year and month from date column

