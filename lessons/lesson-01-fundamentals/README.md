# Lesson 1: Spark Fundamentals

## Learning Objectives

By the end of this lesson, you will be able to:
- Understand the core concepts of distributed computing and Apache Spark
- Install and configure Apache Spark locally
- Create and manipulate Resilient Distributed Datasets (RDDs)
- Understand the difference between transformations and actions
- Execute your first Spark application
- Navigate the Spark UI to monitor job execution

## Prerequisites

- Basic Python programming knowledge
- Understanding of data structures (lists, dictionaries)
- Command line familiarity
- At least 8GB RAM and 4GB free disk space

## Duration

**Estimated Time**: 2-3 hours

## What You'll Learn

### 1. Distributed Computing Concepts
- Why distributed computing matters
- Challenges in distributed systems
- How Spark solves these challenges

### 2. Spark Architecture
- Driver and executor model
- Cluster managers
- Spark components (Core, SQL, Streaming, MLlib, GraphX)

### 3. RDD Fundamentals
- What are RDDs and why they matter
- Creating RDDs from collections and files
- RDD transformations and actions
- Lazy evaluation principles

### 4. Development Environment
- Local Spark installation
- PySpark shell and Jupyter notebooks
- Basic debugging techniques

## Installation Guide

### Option 1: Local Installation (Recommended for Learning)

#### Step 1: Install Java 11
```bash
# macOS with Homebrew
brew install openjdk@11

# Ubuntu/Debian
sudo apt update
sudo apt install openjdk-11-jdk

# Windows
# Download from Oracle or use chocolatey
choco install openjdk11
```

#### Step 2: Install uv (Modern Python Package Manager)
```bash
# Install uv (faster, more reliable than pip)
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or via pip if you prefer
pip install uv

# Verify installation
uv --version
```

#### Step 3: Set Up Project Environment
```bash
# Navigate to the lesson directory
cd lessons/lesson-01-fundamentals

# Create virtual environment and install dependencies
uv venv --python 3.11

# Activate the environment
# macOS/Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate

# Install all dependencies from pyproject.toml
uv sync
```

#### Step 4: Verify Installation
```bash
# Verify PySpark installation
python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"

# Test Jupyter notebook
jupyter notebook --version

# Quick Spark test
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('Test').getOrCreate(); print('Spark is working!'); spark.stop()"
```

#### Step 5: Set Environment Variables
```bash
# Add to your shell profile (.bashrc, .zshrc, etc.)
export JAVA_HOME=/opt/homebrew/opt/openjdk@11  # macOS example
export SPARK_HOME=$(python -c "import pyspark; print(pyspark.__path__[0])")
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

# Or create a .env file in the lesson directory (recommended)
echo "JAVA_HOME=/opt/homebrew/opt/openjdk@11" > .env
echo "SPARK_HOME=$(python -c 'import pyspark; print(pyspark.__path__[0])')" >> .env
```

### Option 2: Docker Installation (Alternative)

#### Step 1: Install Docker
Follow the official Docker installation guide for your platform.

#### Step 2: Pull Spark Docker Image
```bash
# Pull the official Spark image
docker pull apache/spark:3.5.0

# Or use our custom bootcamp image (when available)
docker pull spark-bootcamp:latest
```

#### Step 3: Run Spark Container
```bash
# Run Spark with Jupyter
docker run -it -p 8888:8888 -p 4040:4040 \
  -v $(pwd):/workspace \
  apache/spark:3.5.0 \
  /opt/spark/bin/pyspark
```

### Verification Steps

#### Test 1: uv Environment
```bash
# Ensure you're in the lesson directory with activated environment
cd lessons/lesson-01-fundamentals
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Check uv installation
uv --version

# List installed packages
uv pip list
```

#### Test 2: PySpark Shell
```bash
# Start PySpark shell
pyspark

# You should see the Spark logo and Python prompt
# Try this simple command:
>>> spark.range(10).count()
# Should return: 10
```

#### Test 3: Jupyter Notebook
```bash
# Start Jupyter notebook (will use packages from .venv)
jupyter notebook

# Create a new Python notebook and run:
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestApp").getOrCreate()
print(f"Spark version: {spark.version}")
spark.stop()
```

#### Test 4: Spark UI Access
1. Run a Spark application
2. Open http://localhost:4040 in your browser
3. You should see the Spark Application UI

## Troubleshooting Common Issues

### Issue 1: Java Not Found
**Error**: `JAVA_HOME is not set`
**Solution**:
```bash
# Find Java installation
which java

# Set JAVA_HOME
export JAVA_HOME=$(/usr/libexec/java_home)  # macOS
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  # Linux
```

### Issue 2: Memory Issues
**Error**: `OutOfMemoryError` or slow performance
**Solution**:
```bash
# Increase driver memory
export SPARK_DRIVER_MEMORY=4g

# Or configure in SparkSession
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

### Issue 3: Port Already in Use
**Error**: `Address already in use: 4040`
**Solution**:
```bash
# Find process using port 4040
lsof -i :4040

# Kill the process or use different port
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()
```

### Issue 4: Python Path Issues
**Error**: `Module not found` errors
**Solution**:
```bash
# Ensure you're using the correct virtual environment
source .venv/bin/activate  # macOS/Linux
# or .venv\Scripts\activate  # Windows

# Reinstall packages if needed
uv sync --reinstall

# Check if packages are properly installed
uv pip list | grep pyspark
```

### Issue 5: uv Command Not Found
**Error**: `uv: command not found`
**Solution**:
```bash
# Restart your terminal after installation, or
source ~/.bashrc  # or ~/.zshrc

# Or install via pip as fallback
pip install uv

# Verify installation
which uv
```

## Core Concepts Deep Dive

### 1. What is Apache Spark?

Apache Spark is a unified analytics engine for large-scale data processing. It provides:
- **Speed**: Up to 100x faster than Hadoop MapReduce
- **Ease of Use**: Simple APIs in Java, Scala, Python, and R
- **Generality**: Supports SQL queries, streaming data, ML, and graph processing
- **Runs Everywhere**: YARN, Mesos, Kubernetes, standalone, or in the cloud

### 2. Spark Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Driver Program                          │
│  ┌─────────────────┐    ┌─────────────────┐                   │
│  │   SparkContext  │    │   SparkSession  │                   │
│  │                 │    │                 │                   │
│  └─────────────────┘    └─────────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 │ (commands)
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Cluster Manager                            │
│                   (Standalone/YARN/Mesos)                      │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 │ (resources)
                                 ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Worker 1   │    │   Worker 2   │    │   Worker N   │
│ ┌──────────┐ │    │ ┌──────────┐ │    │ ┌──────────┐ │
│ │Executor 1│ │    │ │Executor 2│ │    │ │Executor N│ │
│ │          │ │    │ │          │ │    │ │          │ │
│ │  Cache   │ │    │ │  Cache   │ │    │ │  Cache   │ │
│ │  Tasks   │ │    │ │  Tasks   │ │    │ │  Tasks   │ │
│ └──────────┘ │    │ └──────────┘ │    │ └──────────┘ │
└──────────────┘    └──────────────┘    └──────────────┘
```

**Key Components:**
- **Driver**: Orchestrates the Spark application
- **Executors**: Run tasks and store data in memory/disk
- **Cluster Manager**: Allocates resources across applications
- **SparkContext**: Entry point for Spark functionality
- **SparkSession**: Unified entry point (Spark 2.0+)

### 3. RDD (Resilient Distributed Dataset)

RDDs are the fundamental data structure of Spark:
- **Resilient**: Fault-tolerant through lineage
- **Distributed**: Partitioned across cluster nodes
- **Dataset**: Collection of objects

**RDD Characteristics:**
- Immutable once created
- Lazy evaluation
- Cacheable in memory
- Partitioned for parallel processing

### 4. Transformations vs Actions

**Transformations** (Lazy):
- Create new RDDs from existing ones
- Not executed until an action is called
- Examples: `map()`, `filter()`, `flatMap()`, `groupBy()`

**Actions** (Eager):
- Trigger computation and return results
- Execute the entire RDD lineage
- Examples: `collect()`, `count()`, `first()`, `save()`

## Hands-On Examples

### Example 1: Creating Your First RDD

```python
from pyspark import SparkContext, SparkConf

# Create Spark configuration
conf = SparkConf().setAppName("FirstSparkApp").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Create RDD from a Python list
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
numbers_rdd = sc.parallelize(numbers)

print(f"Number of partitions: {numbers_rdd.getNumPartitions()}")
print(f"First element: {numbers_rdd.first()}")
print(f"Total count: {numbers_rdd.count()}")

# Stop SparkContext
sc.stop()
```

### Example 2: Basic Transformations

```python
from pyspark.sql import SparkSession

# Modern way using SparkSession
spark = SparkSession.builder \
    .appName("TransformationsExample") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# Create RDD
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(data)

# Transformation: multiply each number by 2
doubled_rdd = rdd.map(lambda x: x * 2)

# Transformation: filter even numbers
even_rdd = doubled_rdd.filter(lambda x: x % 2 == 0)

# Action: collect results
result = even_rdd.collect()
print(f"Doubled even numbers: {result}")

spark.stop()
```

### Example 3: Working with Text Data

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TextProcessing") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# Create RDD from text data
text_data = [
    "Apache Spark is a unified analytics engine",
    "Spark provides high-level APIs in Java, Scala, Python and R",
    "Spark supports SQL queries, streaming data, machine learning and graph processing"
]

text_rdd = sc.parallelize(text_data)

# Split text into words
words_rdd = text_rdd.flatMap(lambda line: line.split())

# Convert to lowercase and filter out short words
clean_words_rdd = words_rdd.map(lambda word: word.lower()) \
                           .filter(lambda word: len(word) > 3)

# Count word occurrences
word_counts = clean_words_rdd.map(lambda word: (word, 1)) \
                            .reduceByKey(lambda a, b: a + b)

# Get results
results = word_counts.collect()
for word, count in sorted(results):
    print(f"{word}: {count}")

spark.stop()
```

## File Structure

Your lesson directory should look like this:

```
lesson-01-fundamentals/
├── README.md                    # This file
├── notebook.ipynb              # Interactive Jupyter notebook
├── data/
│   ├── sample_data.txt         # Sample text file for exercises
│   └── numbers.csv             # Sample CSV file
├── exercises/
│   ├── exercise_1.py           # Basic RDD operations
│   ├── exercise_2.py           # Text processing challenge
│   └── exercise_3.py           # Word count implementation
├── solutions/
│   ├── exercise_1_solution.py  # Solution to exercise 1
│   ├── exercise_2_solution.py  # Solution to exercise 2
│   └── exercise_3_solution.py  # Solution to exercise 3
└── tests/
    ├── test_basics.py          # Unit tests for basic concepts
    └── test_exercises.py       # Tests for exercise validation
```

## Running the Lesson

### Step 1: Set Up Environment
```bash
# Navigate to lesson directory
cd lessons/lesson-01-fundamentals

# Create and activate virtual environment with uv
uv venv --python 3.11
source .venv/bin/activate  # macOS/Linux
# or .venv\Scripts\activate  # Windows

# Install all dependencies
uv sync

# Copy environment template and configure
cp .env.example .env
# Edit .env file with your system-specific paths
```

### Step 2: Start Jupyter Notebook
```bash
# Start Jupyter (uses packages from .venv)
jupyter notebook notebook.ipynb

# Or start Jupyter lab for a modern interface
jupyter lab notebook.ipynb
```

### Step 3: Complete Exercises
```bash
# Run individual exercises
python exercises/exercise_1.py
python exercises/exercise_2.py
python exercises/exercise_3.py

# Or use the convenient scripts defined in pyproject.toml
uv run spark-exercise-1
uv run spark-exercise-2
uv run spark-exercise-3

# Run all exercises with pytest
uv run pytest exercises/ -v
```

### Step 4: Validate Your Learning
```bash
# Run all validation tests
uv run pytest tests/ -v

# Run specific test files
uv run pytest tests/test_basics.py -v
uv run pytest tests/test_exercises.py -v

# Run tests with coverage report
uv run pytest --cov=exercises --cov=solutions tests/
```

## Key Takeaways

1. **Spark is Fast**: In-memory computing makes it much faster than traditional disk-based systems
2. **RDDs are Immutable**: Once created, RDDs cannot be changed, only transformed into new RDDs
3. **Lazy Evaluation**: Transformations aren't executed until an action is called
4. **Fault Tolerance**: RDDs can be recreated using their lineage if a partition is lost
5. **Distributed by Default**: Data is automatically partitioned across available cores/nodes

## Next Steps

After completing this lesson, you should:
1. Be comfortable creating and manipulating RDDs
2. Understand the difference between transformations and actions
3. Have a working Spark development environment

**Ready for Lesson 2?**
In the next lesson, we'll dive into DataFrames and Spark SQL, which provide a more structured and optimized way to work with data.

## Additional Resources

- [Official Spark Documentation](https://spark.apache.org/docs/latest/)
- [Spark Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [PySpark API Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Spark Examples](https://github.com/apache/spark/tree/master/examples)

## Getting Help

If you encounter issues:
1. Check the troubleshooting section above
2. Review the validation tests for hints
3. Consult the additional resources
4. Ask questions in the course discussion forum

Remember: The best way to learn Spark is by doing. Don't just read the examples—run them, modify them, and experiment!
