## 🔹 Objective

The main objective of this phase was to perform **data cleaning, transformation, and pattern extraction** using **PySpark and Regular Expressions (Regex)**.
The focus was on handling real-world data problems like inconsistent formats, missing values, and extracting useful information from text data.

---

## 🔹 Problem Summary

The dataset contained several real-world issues:

* Mixed data patterns (alphabets + numbers in the same column)
* Columns like **email, phone, and IDs** needing pattern extraction
* Missing (NULL) values in multiple columns
* Inconsistent formatting in categorical data

---

## 🔹 Approach

To solve these issues, the following steps were followed:

1. Loaded the dataset into a PySpark DataFrame
2. Explored the data to identify inconsistencies and NULL values
3. Applied filters to extract required records
4. Performed column transformations to standardize data
5. Used Regex to extract patterns from text fields
6. Applied sorting and removed duplicates
7. Generated a clean and structured final dataset

---

## 🔹 Key Transformations Used

### 🔸 Regular Expressions (Regex)

Used for extracting patterns such as:

* Numbers from strings
* Values at the beginning or end
* Email components (username, domain)
* Phone number country codes
* Separating alphabets and numbers

👉 **Importance:** Helps convert unstructured text into meaningful structured data.

---

### 🔸 Data Filtering

* Applied conditions to filter relevant rows
* Used multiple logical conditions

👉 **Importance:** Reduces dataset size and keeps only useful data.

---

### 🔸 Column Transformations

* Renamed columns
* Created new derived columns
* Standardized values

👉 **Importance:** Improves readability and consistency.

---

### 🔸 Conditional Logic

* Used conditions to modify values
* Categorized data

👉 **Importance:** Applies business rules to data.

---

### 🔸 NULL Handling

* Identified missing values
* Replaced with default values

👉 **Importance:** Prevents errors and ensures accurate processing.

---

### 🔸 Sorting & Deduplication

* Sorted data based on columns
* Removed duplicate rows

👉 **Importance:** Keeps data clean and organized.

---

### 🔸 Date Functions

* Added current date
* Performed date calculations

👉 **Importance:** Useful for time-based analysis.

---

### 🔸 String Operations

* Converted text case (upper/lower)
* Split columns

👉 **Importance:** Makes text data consistent and usable.

---

## 🔹 Output / Results

After processing, the dataset became:

* Clean and standardized
* Free from NULL issues
* Enriched with extracted patterns:

  * Email username & domain
  * Phone country codes
  * Numeric & alphabetic parts
* Sorted and deduplicated
* Ready for analysis

---

## 🔹 Data Engineering Considerations

* Proper handling of NULL values
* Careful use of Regex to avoid wrong extraction
* Maintaining consistency after transformations
* Validating results after each step

---

## 🔹 Challenges Faced

* Writing and understanding complex Regex
* Extracting exact patterns without errors
* Handling mixed data formats
* Managing NULL values correctly

---

## 🔹 Learnings

* Practical use of **Regex in data engineering**
* Importance of **data cleaning in real-world datasets**
* Hands-on experience with **PySpark transformations**
* Techniques to handle missing data
* Applying logic to standardize datasets

---

## 🔹 Topics Covered

* Regular Expressions (Pattern Matching & Extraction)
* Data Filtering & Transformations
* NULL Handling
* String Manipulation
* Date Functions
* Sorting & Deduplication

