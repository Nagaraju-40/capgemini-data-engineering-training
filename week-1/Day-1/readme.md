
## 🎯 Objective

To perform data cleaning and preprocessing using PySpark by identifying and resolving common data quality issues such as duplicates, null values, inconsistent formats, and invalid entries, making the dataset ready for analysis.

---

## 📌 Problem Summary

The given dataset contains multiple data quality problems:

* Duplicate records
* Missing (`NULL`) values
* Invalid date formats (e.g., `wrong_date`, `blank`)
* Inconsistent text values (e.g., `India` vs `india`)
* Incorrect data types (e.g., `Sales` as string instead of numeric)

These issues make the dataset unreliable for analytics and reporting.

---

## ⚠️ Key Challenges

* Handling mixed data formats in the same column
* Cleaning invalid string values like `"blank"`
* Converting string dates into proper date format
* Ensuring no loss of important data while removing nulls
* Maintaining consistency across categorical columns

---

## 🛠️ Approach

1. Load the dataset using PySpark
2. Perform initial exploration (`show()`, `printSchema()`)
3. Identify data quality issues
4. Apply cleaning techniques step by step:

   * Remove duplicates
   * Handle null values
   * Fix date formats
   * Remove invalid records
5. Validate the cleaned dataset

---

## 🔄 Key Transformations Used

* `dropDuplicates()` → Remove duplicate rows
* `dropna()` → Handle null values
* `withColumn()` → Modify and transform columns
* `try_to_date()` → Convert string to date safely
* `col()` → Reference columns for transformations

---

## 📚 Learnings

* Importance of data cleaning before analysis
* Handling real-world messy datasets in PySpark
* Efficient use of built-in functions for transformation
* Managing schema inconsistencies
* Writing scalable data processing logic

---

## 📊 Output

* Cleaned dataset with:

  * No duplicate records
  * No null or invalid values
  * Properly formatted dates
  * Improved consistency in data

* Data is now:
  ✅ Reliable
  ✅ Structured
  ✅ Ready for analysis or further processing

