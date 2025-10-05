# Lesson 2: DataFrames & Spark SQL

## Learning Objectives

By the end of this lesson, you will be able to:
- Understand the DataFrame API and how it differs from RDDs
- Create DataFrames from various data sources
- Perform complex data transformations using DataFrame operations
- Write and execute Spark SQL queries
- Understand schema inference and definition
- Work with different data types and column operations
- Optimize DataFrame operations using the Catalyst optimizer
- Join multiple DataFrames efficiently
- Transition from RDD-based thinking to DataFrame-based thinking

## Prerequisites

- **Completion of Lesson 1**: Understanding of RDDs, transformations, and actions
- **Basic SQL Knowledge**: Familiarity with SELECT, WHERE, GROUP BY, JOIN operations
- **Python Skills**: Comfortable with Python data structures and functions
- **Working Spark Environment**: From Lesson 1 setup

## Duration

**Estimated Time**: 3-4 hours

## What You'll Learn

### 1. DataFrame Fundamentals
- What are DataFrames and why they're better than RDDs for structured data
- DataFrame architecture and the Catalyst optimizer
- Creating DataFrames from different sources
- Schema inference vs explicit schema definition

### 2. Basic DataFrame Operations
- Selecting and filtering data
- Column operations and transformations
- Basic aggregations and grouping
- Sorting and limiting results

### 3. Spark SQL
- Creating temporary views
- Writing SQL queries against DataFrames
- Combining SQL and DataFrame operations
- Advanced SQL features (window functions, CTEs)

### 4. Working with Multiple DataFrames
- Different types of joins
- Union operations
- Handling null values and data quality

### 5. Performance and Optimization
- Understanding query execution plans
- Caching strategies for DataFrames
- When to use DataFrames vs RDDs

## Installation and Setup

### Step 1: Navigate to Lesson Directory
```bash
cd lessons/lesson-02-dataframes
```

### Step 2: Set Up Environment with Enhanced Dependencies
```bash
# Create virtual environment
uv venv --python 3.11
source .venv/bin/activate  # macOS/Linux
# or .venv\Scripts\activate  # Windows

# Install dependencies (includes visualization libraries)
uv sync

# Copy and configure environment
cp .env.example .env
# Edit .env file with your system-specific paths
```

### Step 3: Verify Setup
```bash
# Test DataFrame functionality
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('TestDataFrames').getOrCreate()
df = spark.range(5).toDF('number')
df.show()
spark.stop()
print('✅ DataFrames working correctly!')
"
```

## Key Concepts Deep Dive

### 1. DataFrames vs RDDs

**RDDs (From Lesson 1)**:
- Low-level, functional programming interface
- Work with any Python object
- No automatic optimization
- Manual schema management

**DataFrames**:
- High-level, SQL-like interface
- Structured data with schema
- Automatic query optimization (Catalyst)
- Better performance and memory usage

```python
# RDD approach (Lesson 1)
rdd = sc.parallelize([("Alice", 25), ("Bob", 30)])
result = rdd.filter(lambda x: x[1] > 27).collect()

# DataFrame approach (Lesson 2)
df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
result = df.filter(df.age > 27).collect()
```

### 2. The Catalyst Optimizer

Catalyst is Spark's query optimizer that:
- **Analyzes queries** and creates optimized execution plans
- **Pushes down predicates** (filters) for better performance
- **Eliminates unnecessary operations** and combines operations
- **Chooses optimal join strategies** based on data size

```python
# View the execution plan
df.filter(df.age > 25).explain(True)
```

### 3. Schema Management

**Schema Inference** (Automatic):
```python
# Spark infers schema from data
df = spark.read.json("data/people.json")
df.printSchema()
```

**Explicit Schema** (Recommended for production):
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.read.schema(schema).json("data/people.json")
```

### 4. DataFrame Creation Methods

```python
# From Python collections
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# From files
df = spark.read.csv("data/people.csv", header=True, inferSchema=True)
df = spark.read.json("data/people.json")
df = spark.read.parquet("data/people.parquet")

# From RDDs (migration from Lesson 1)
rdd = sc.parallelize(data)
df = rdd.toDF(["name", "age"])

# From SQL queries
spark.sql("SELECT * FROM people WHERE age > 25")
```

## Core DataFrame Operations

### 1. Selection and Projection

```python
# Select specific columns
df.select("name", "age").show()
df.select(df.name, df.age + 1).show()

# Column operations
from pyspark.sql.functions import col, upper, length
df.select(
    col("name"),
    upper(col("name")).alias("name_upper"),
    length(col("name")).alias("name_length")
).show()
```

### 2. Filtering

```python
# Basic filtering
df.filter(df.age > 25).show()
df.filter(col("age") > 25).show()
df.where(df.age.between(25, 35)).show()

# Multiple conditions
df.filter((df.age > 25) & (df.name.startswith("A"))).show()
```

### 3. Aggregations

```python
# Basic aggregations
df.count()
df.agg({"age": "avg", "name": "count"}).show()

# Group by operations
df.groupBy("department").agg(
    avg("salary").alias("avg_salary"),
    count("*").alias("employee_count")
).show()
```

### 4. Sorting

```python
# Single column sorting
df.orderBy("age").show()
df.orderBy(df.age.desc()).show()

# Multiple column sorting
df.orderBy("department", df.age.desc()).show()
```

## Spark SQL Integration

### 1. Creating Temporary Views

```python
# Create a temporary view
df.createOrReplaceTempView("people")

# Now you can use SQL
result = spark.sql("""
    SELECT name, age,
           CASE WHEN age < 30 THEN 'Young'
                WHEN age < 50 THEN 'Middle'
                ELSE 'Senior' END as age_group
    FROM people
    WHERE age > 20
    ORDER BY age
""")
result.show()
```

### 2. Advanced SQL Operations

```python
# Window functions
spark.sql("""
    SELECT name, age,
           ROW_NUMBER() OVER (ORDER BY age DESC) as rank,
           AVG(age) OVER () as avg_age
    FROM people
""").show()

# Common Table Expressions (CTEs)
spark.sql("""
    WITH age_stats AS (
        SELECT AVG(age) as avg_age, MAX(age) as max_age
        FROM people
    )
    SELECT p.name, p.age,
           p.age - a.avg_age as age_diff
    FROM people p
    CROSS JOIN age_stats a
""").show()
```

## Working with Multiple DataFrames

### 1. Joins

```python
# Sample datasets
employees = spark.createDataFrame([
    (1, "Alice", "Engineering"),
    (2, "Bob", "Marketing"),
    (3, "Charlie", "Engineering")
], ["id", "name", "department"])

salaries = spark.createDataFrame([
    (1, 80000),
    (2, 75000),
    (3, 85000)
], ["emp_id", "salary"])

# Inner join
result = employees.join(salaries, employees.id == salaries.emp_id, "inner")
result.show()

# Different join types
left_join = employees.join(salaries, employees.id == salaries.emp_id, "left")
right_join = employees.join(salaries, employees.id == salaries.emp_id, "right")
full_join = employees.join(salaries, employees.id == salaries.emp_id, "outer")
```

### 2. Union Operations

```python
# Union DataFrames with same schema
df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df2 = spark.createDataFrame([(3, "Charlie"), (4, "Diana")], ["id", "name"])

combined = df1.union(df2)
combined.show()
```

## Performance Optimization

### 1. Caching DataFrames

```python
# Cache a DataFrame that will be used multiple times
df.cache()

# Force materialization
df.count()

# Use the cached DataFrame
df.filter(df.age > 25).show()
df.groupBy("department").count().show()

# Clean up cache when done
df.unpersist()
```

### 2. Understanding Execution Plans

```python
# Physical execution plan
df.filter(df.age > 25).explain()

# Extended execution plan (shows all optimization stages)
df.filter(df.age > 25).explain(True)
```

### 3. Broadcast Joins

```python
from pyspark.sql.functions import broadcast

# Broadcast small DataFrame for efficient joins
large_df.join(broadcast(small_df), "join_key").show()
```

## Data Types and Conversions

### 1. Working with Different Data Types

```python
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from pyspark.sql.functions import *

# String operations
df.select(
    upper(col("name")),
    length(col("name")),
    substring(col("name"), 1, 3)
).show()

# Numeric operations
df.select(
    col("age"),
    col("age") + 5,
    round(col("salary") / 1000, 2).alias("salary_k")
).show()

# Date operations
df.select(
    current_date().alias("today"),
    date_add(current_date(), 30).alias("future_date"),
    year(col("birth_date")).alias("birth_year")
).show()
```

### 2. Handling Null Values

```python
# Drop rows with null values
df.dropna().show()

# Fill null values
df.fillna({"age": 0, "name": "Unknown"}).show()

# Check for null values
df.filter(col("age").isNull()).show()
df.filter(col("age").isNotNull()).show()
```

## File Structure

Your lesson directory should look like this:

```
lesson-02-dataframes/
├── README.md                    # This file
├── notebook.ipynb              # Interactive Jupyter notebook
├── pyproject.toml              # uv dependencies with visualization libs
├── .python-version             # Python version specification
├── .env.example                # Environment template
├── .gitignore                  # Git ignore patterns
├── Makefile                    # Development commands
├── data/
│   ├── sales_data.csv          # E-commerce transactions
│   ├── customers.json          # Customer profiles
│   ├── products.parquet        # Product catalog
│   ├── web_logs.txt           # Server logs
│   └── employees.csv          # Employee data for joins
├── exercises/
│   ├── exercise_1.py          # DataFrame creation
│   ├── exercise_2.py          # Basic operations
│   ├── exercise_3.py          # SQL queries
│   ├── exercise_4.py          # Joins and combinations
│   ├── exercise_5.py          # Column functions
│   ├── exercise_6.py          # Analytics and aggregations
│   └── exercise_7.py          # Performance comparison
├── solutions/
│   ├── exercise_1_solution.py # Solutions for exercise 1
│   ├── exercise_2_solution.py # Solutions for exercise 2
│   ├── exercise_3_solution.py # Solutions for exercise 3
│   ├── exercise_4_solution.py # Solutions for exercise 4
│   ├── exercise_5_solution.py # Solutions for exercise 5
│   ├── exercise_6_solution.py # Solutions for exercise 6
│   └── exercise_7_solution.py # Solutions for exercise 7
└── tests/
    ├── test_basics.py         # DataFrame concept tests
    └── test_exercises.py      # Exercise validation tests
```

## Running the Lesson

### Step 1: Set Up Environment
```bash
# Navigate to lesson directory
cd lessons/lesson-02-dataframes

# Create and activate virtual environment
uv venv --python 3.11
source .venv/bin/activate  # macOS/Linux
# or .venv\Scripts\activate  # Windows

# Install all dependencies
uv sync

# Copy environment template
cp .env.example .env
```

### Step 2: Start Learning
```bash
# Start with the interactive notebook
jupyter notebook notebook.ipynb

# Or use Jupyter Lab for a modern interface
jupyter lab
```

### Step 3: Complete Exercises
```bash
# Run individual exercises
python exercises/exercise_1.py
python exercises/exercise_2.py
# ... through exercise_7.py

# Or use convenient make commands
make run-exercise-1
make run-exercise-2
# ... etc

# Run all exercises
make run-exercises
```

### Step 4: Validate Learning
```bash
# Run all validation tests
make test

# Run specific test suites
uv run pytest tests/test_basics.py -v
uv run pytest tests/test_exercises.py -v

# Check your solutions against reference
make run-solutions
```

## Troubleshooting

### Issue 1: DataFrame Schema Errors
**Error**: `AnalysisException: cannot resolve column`
**Solution**:
```bash
# Check DataFrame schema
df.printSchema()

# Use correct column references
df.select(col("column_name"))  # instead of df.column_name if column doesn't exist
```

### Issue 2: Memory Issues with Large DataFrames
**Error**: `OutOfMemoryError` or slow performance
**Solution**:
```bash
# Increase driver memory in your SparkSession
spark = SparkSession.builder \
    .appName("DataFrameApp") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
```

### Issue 3: SQL Syntax Errors
**Error**: `ParseException` in SQL queries
**Solution**:
```python
# Use DataFrame API first, then translate to SQL
result_df = df.filter(col("age") > 25).select("name", "age")
result_df.show()

# Then write equivalent SQL
spark.sql("SELECT name, age FROM people WHERE age > 25").show()
```

### Issue 4: Join Performance Issues
**Error**: Slow join operations
**Solution**:
```python
# Use broadcast for small DataFrames
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), "key")

# Check execution plan
df.explain()
```

## Key Takeaways

1. **DataFrames are Higher-Level**: They provide better performance and optimization than RDDs
2. **Schema Matters**: Understanding your data structure improves performance and prevents errors
3. **Catalyst Optimizer**: Spark automatically optimizes your queries
4. **SQL Integration**: You can mix DataFrame API and SQL seamlessly
5. **Performance**: Use caching, broadcast joins, and explain plans for optimization
6. **Data Types**: Understanding Spark data types helps with transformations
7. **Null Handling**: Always consider null values in your data processing logic

## Migration from RDDs (Lesson 1)

| RDD Operation | DataFrame Equivalent | SQL Equivalent |
|---------------|---------------------|----------------|
| `rdd.map(f)` | `df.select(f(col))` | `SELECT f(col) FROM table` |
| `rdd.filter(f)` | `df.filter(f(col))` | `SELECT * FROM table WHERE f(col)` |
| `rdd.reduce(f)` | `df.agg(f(col))` | `SELECT f(col) FROM table` |
| `rdd.groupByKey()` | `df.groupBy(col)` | `SELECT col, ... FROM table GROUP BY col` |
| `rdd.join(other)` | `df.join(other, key)` | `SELECT * FROM df JOIN other ON key` |

## Next Steps

After completing this lesson, you should:
1. Be comfortable creating and manipulating DataFrames
2. Write basic to intermediate SQL queries against your data
3. Understand when to use DataFrames vs RDDs
4. Know how to optimize DataFrame operations

**Ready for Lesson 3?**
In the next lesson, we'll dive into local development setup, IDE configuration, and professional development workflows for Spark applications.

## Additional Resources

- [Spark SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [DataFrame API Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html)
- [Spark SQL Functions](https://spark.apache.org/docs/latest/api/sql/index.html)
- [Catalyst Optimizer Deep Dive](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)

## Getting Help

If you encounter issues:
1. Check the troubleshooting section above
2. Review the validation tests for hints
3. Compare your solutions with the provided solutions
4. Consult the additional resources
5. Use `.explain()` to understand query execution

Remember: DataFrames are more intuitive than RDDs for most data processing tasks. Focus on thinking in terms of tables and SQL operations!
