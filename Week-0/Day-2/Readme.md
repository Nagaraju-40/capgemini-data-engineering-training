# 📊 SQL JOIN Practice Project

---

## 🎯 Objective

The main objective of this project is to understand and practice different types of SQL JOIN operations such as INNER JOIN, LEFT JOIN, RIGHT JOIN, and FULL OUTER JOIN using real-world employee, department, and project datasets.

---

## 📌 Problem Statement

Given three tables — **employees, departments, and projects**, solve multiple SQL queries to:

* Retrieve meaningful relationships between data
* Handle missing data using joins
* Ensure inclusion/exclusion of records based on conditions
* Simulate real-world business scenarios

The tasks include:

* Finding employee-manager relationships
* Mapping employees to departments and projects
* Identifying missing relationships (NULL cases)
* Aggregating and filtering data

---

## ⚙️ Approach

* Designed relational tables: `employees`, `departments`, and `projects`
* Used different JOIN types:

  * INNER JOIN → for matching records
  * LEFT JOIN → to include all records from left table
  * RIGHT JOIN → to include all records from right table
  * FULL OUTER JOIN → to include all records from both tables
* Applied:

  * `WHERE` conditions for filtering
  * `GROUP BY` and `HAVING` for aggregation
  * `COALESCE` to handle NULL values
* Solved 30 real-time SQL queries step by step

---

## ⚠️ Challenges Faced

* Understanding when to use different types of JOINs
* Handling NULL values correctly
* Simulating FULL OUTER JOIN in MySQL (using UNION)
* Designing queries for missing relationships
* Managing many-to-many relationships (required junction table concept)

---

## 📚 Learnings

* Strong understanding of SQL JOIN concepts
* Difference between INNER, LEFT, RIGHT, and FULL JOIN
* How to handle missing data using NULL
* Real-world database design concepts
* Writing optimized and readable SQL queries
* Importance of table relationships in databases

---

## 📊 Output

* Successfully executed all 30 SQL queries
* Generated meaningful outputs such as:

  * Employee-manager mapping
  * Department-wise employee listing
  * Project assignments
  * Employees without departments/projects
* Covered both matching and non-matching data scenarios

---

## 🛠️ Technologies Used

* SQL (MySQL / Databricks SQL)
* Jupyter Notebook / Databricks Notebook (.ipynb)
* Database concepts (Relational Model)

---

## 🚀 Conclusion

This project provides a complete hands-on understanding of SQL JOIN operations and their real-world applications. It strengthens problem-solving skills and prepares for interviews and practical database scenarios.

---



# 📊 SQL GROUP BY Practice Project

---

## 🎯 Objective

The objective of this project is to understand and implement SQL aggregation techniques using the **GROUP BY clause**, along with filtering using **WHERE** and **HAVING** conditions.

---

## 📌 Problem Statement

Given an **Employee table** containing employee details such as name, department, salary, and joining date, the goal is to:

* Perform aggregations like **SUM, COUNT, AVG, MAX, MIN**
* Group data based on departments
* Apply conditions on grouped data using **HAVING**
* Analyze employee distribution and salary patterns across departments

The project includes solving multiple queries involving:

* Basic grouping
* Conditional filtering
* Advanced grouping using HAVING

---

## ⚙️ Approach

* Created the `Employee` table with relevant attributes
* Inserted sample data for multiple departments
* Used SQL aggregation functions:

  * `SUM()` → total salary
  * `COUNT()` → number of employees
  * `AVG()` → average salary
  * `MAX()` and `MIN()` → salary range
* Applied:

  * `GROUP BY` to organize data by department
  * `WHERE` for row-level filtering
  * `HAVING` for group-level filtering
* Solved 15 queries covering basic to advanced scenarios

---

## ⚠️ Challenges Faced

* Understanding the difference between **WHERE and HAVING**
* Applying conditions after grouping correctly
* Handling edge cases in aggregation queries
* Ensuring accurate grouping without missing data
* Writing optimized queries for readability and performance

---

## 📚 Learnings

* Clear understanding of **GROUP BY clause**
* Difference between **row-level filtering (WHERE)** and **group-level filtering (HAVING)**
* Use of aggregate functions in real-world scenarios
* Data analysis using SQL
* Writing clean and efficient SQL queries
* Handling business logic using aggregation

---

## 📊 Output

* Successfully executed all GROUP BY queries
* Generated insights such as:

  * Department-wise salary totals
  * Employee count per department
  * Highest and lowest salaries
  * Filtered departments based on salary thresholds
* Demonstrated real-world data analysis use cases

---

## 🛠️ Technologies Used

* SQL (MySQL / Databricks SQL)
* Jupyter Notebook / Databricks Notebook (.ipynb)
* Relational Database Concepts

---

## 🚀 Conclusion

This project provides hands-on experience with SQL aggregation and grouping techniques. It strengthens analytical thinking and prepares for real-world data analysis and SQL interview questions.

---
