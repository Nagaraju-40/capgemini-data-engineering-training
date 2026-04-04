 #-------------------------------------------------
 Phase 3A – Data Quality AND Cleaning using PySpark
 #-------------------------------------------------
 
 Objective

Work with intentionally messy data and apply cleaning techniques before building a pipeline. This phase
helps you understand why data cleaning is critical in real-world data engineering.

Problem Statement

data = [
(1, "Ravi", "Hyderabad", 25),
(2, None, "Chennai", 32),
(None, "Arun", "Hyderabad", 28),
(4, "Meena", None, 30),
(4, "Meena", None, 30),
(5, "John", "Bangalore", -5)
]
columns = ["customer_id", "name", "city", "age"]
df = spark.createDataFrame(data, columns)

Raw datasets often contain:

Missing values
Duplicate records
Invalid data (e.g., negative age)

🧠 Approach
Initialize Spark Session
Create a sample dataset with intentional data issues
Perform data validation (schema + row count)
Apply multiple cleaning techniques
Validate cleaned data
Perform aggregation to derive insights

⚠️ Challenges Faced
🔸 Handling Missing Values

Identifying which columns are critical (like customer_id) and deciding whether to remove or fill missing data.

🔸 Duplicate Data

Duplicate rows can distort analysis results. Needed careful removal without losing valid data.

🔸 Invalid Data Detection

Negative values (e.g., age = -5) required validation rules.

🔸 Data Quality Awareness

Understanding that raw data is often inconsistent and needs preprocessing before analysis.

📚 Learnings
Importance of data cleaning in data engineering
Practical usage of:
dropna()
fillna()
dropDuplicates()
filter()
How to validate datasets using:
.printSchema()
.count()
Real-world understanding of data quality issues
Real-world data is messy

- Cleaning is mandatory before processing
- Invalid data leads to wrong results
- Validation is essential

📈 Output
🔹 Cleaned Dataset
Removed null customer_id
Replaced missing values with defaults
Removed duplicate rows
Filtered invalid ages