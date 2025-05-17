# PySpark Reference Card & Cheat Sheet

## Table of Contents
- [PySpark Basics](#pyspark-basics)
- [DataFrame Operations](#dataframe-operations)
- [Data Types](#data-types)
- [Transformations & Actions](#transformations--actions)
- [SQL Functions](#sql-functions)
- [Window Functions](#window-functions)
- [Machine Learning (MLlib)](#machine-learning-mllib)
- [Streaming](#streaming)
- [Performance Optimization](#performance-optimization)
- [Snowflake Integration](#snowflake-integration)

## PySpark Basics

### Installation
```bash
pip install pyspark
pip install pyspark[sql]  # For SQL support
pip install pyspark[ml]   # For MLlib support
```

### Creating a SparkSession
```python
from pyspark.sql import SparkSession

# Basic SparkSession
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .getOrCreate()

# SparkSession with advanced configuration
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "100") \
    .master("local[*]") \  # For local mode with all available cores
    .enableHiveSupport() \ # For Hive support
    .getOrCreate()
```

### Accessing SparkContext and SQL Context
```python
# Access SparkContext from SparkSession
sc = spark.sparkContext

# Access SQLContext
sql_context = spark.sqlContext
```

### Creating RDDs (Resilient Distributed Datasets)
```python
# Create RDD from a list
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Create RDD from a file
rdd_from_file = sc.textFile("path/to/file.txt")

# Create RDD from multiple files
rdd_from_dir = sc.wholeTextFiles("path/to/directory/")
```

## DataFrame Operations

### Creating DataFrames
```python
# From a list of data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# From RDD
rdd = sc.parallelize([("Alice", 25), ("Bob", 30)])
df = spark.createDataFrame(rdd, ["name", "age"])

# From Pandas DataFrame
import pandas as pd
pandas_df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
df = spark.createDataFrame(pandas_df)

# Read from various data sources
df_csv = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
df_json = spark.read.json("path/to/file.json")
df_parquet = spark.read.parquet("path/to/file.parquet")
df_orc = spark.read.orc("path/to/file.orc")
df_table = spark.read.table("database.table_name")
```

### Basic DataFrame Operations
```python
# Show data
df.show()  # Default shows 20 rows
df.show(n=5, truncate=False)  # Show 5 rows without truncation

# Show schema
df.printSchema()

# Select columns
df.select("name", "age").show()
df.select(df["name"], df["age"] + 1).show()  # Using expressions

# Filter rows
df.filter(df["age"] > 25).show()
df.filter("age > 25").show()  # Using SQL expression

# Sort rows
df.sort("age").show()
df.sort(df["age"].desc()).show()
df.orderBy("age").show()

# Add/modify columns
df.withColumn("age_plus_10", df["age"] + 10).show()
df.withColumnRenamed("age", "years").show()

# Drop columns
df.drop("age").show()

# Distinct values
df.distinct().show()
df.select("age").distinct().show()

# Count & Summary
df.count()  # Return number of rows
df.describe().show()  # Statistical summary of numeric columns
df.describe("age", "height").show()  # Summary of specific columns
```

### Grouping & Aggregation
```python
from pyspark.sql import functions as F

# Group by and count
df.groupBy("age").count().show()

# Multiple aggregations
df.groupBy("department").agg(
    F.count("id").alias("count"),
    F.avg("salary").alias("avg_salary"),
    F.min("salary").alias("min_salary"),
    F.max("salary").alias("max_salary"),
    F.sum("salary").alias("total_salary")
).show()

# Pivot tables
df.groupBy("department").pivot("year").sum("salary").show()
```

### Joins
```python
# Inner join (default)
df1.join(df2, df1["id"] == df2["id"]).show()

# Specify join type
df1.join(df2, df1["id"] == df2["id"], "inner").show()
df1.join(df2, df1["id"] == df2["id"], "left").show()
df1.join(df2, df1["id"] == df2["id"], "right").show()
df1.join(df2, df1["id"] == df2["id"], "outer").show()
df1.join(df2, df1["id"] == df2["id"], "leftsemi").show()
df1.join(df2, df1["id"] == df2["id"], "leftanti").show()

# Cross join
df1.crossJoin(df2).show()

# Join with multiple conditions
df1.join(df2, (df1["id"] == df2["id"]) & (df1["dept"] == df2["dept"])).show()
```

### Union and Intersect
```python
# Union (combine rows, keep duplicates)
df1.union(df2).show()

# Union with distinct rows
df1.union(df2).distinct().show()
df1.unionByName(df2)  # Union by column names instead of position

# Intersect (common rows)
df1.intersect(df2).show()

# Except/Subtract (rows in df1 but not in df2)
df1.subtract(df2).show()
```

### Writing DataFrames
```python
# Write to various formats
df.write.csv("path/to/output.csv")
df.write.json("path/to/output.json")
df.write.parquet("path/to/output.parquet")
df.write.orc("path/to/output.orc")

# Specify save mode
# 'error' (default), 'append', 'overwrite', 'ignore'
df.write.mode("overwrite").parquet("path/to/output")

# Write with partitioning
df.write.partitionBy("year", "month").parquet("path/to/output")

# Write to Hive table
df.write.saveAsTable("database.table_name")
```

### Converting to/from Pandas
```python
# To Pandas
pandas_df = df.toPandas()

# From Pandas
spark_df = spark.createDataFrame(pandas_df)
```

## Data Types

### Basic Types
```python
from pyspark.sql.types import *

# String types
StringType()
VarcharType(length)
CharType(length)

# Numeric types
ByteType()
ShortType()
IntegerType()
LongType()
FloatType()
DoubleType()
DecimalType(precision, scale)

# Boolean type
BooleanType()

# Date/Time types
DateType()
TimestampType()

# Binary type
BinaryType()

# Complex types
ArrayType(elementType)
MapType(keyType, valueType)
StructType([StructField("field_name", dataType, nullable)])
```

### Creating a Schema
```python
schema = StructType([
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), True),
    StructField("height", FloatType(), True),
    StructField("addresses", ArrayType(
        StructType([
            StructField("city", StringType(), True),
            StructField("zip", StringType(), True)
        ])
    ), True)
])

# Create DataFrame with schema
df = spark.createDataFrame(data, schema)
df = spark.read.schema(schema).csv("path/to/file.csv")
```

### Type Casting
```python
from pyspark.sql.functions import col

# Cast column types
df.withColumn("age", col("age").cast(IntegerType()))
df.withColumn("age", col("age").cast("integer"))  # Using string notation

# Common string casts
df.withColumn("price", col("price_str").cast("double"))
df.withColumn("date", col("date_str").cast("date"))
df.withColumn("timestamp", col("ts_str").cast("timestamp"))
```

## Transformations & Actions

### Key RDD Transformations
```python
# Transformations (lazy operations)
mapped_rdd = rdd.map(lambda x: x * 2)
filtered_rdd = rdd.filter(lambda x: x > 10)
flatmapped_rdd = rdd.flatMap(lambda x: [x, x+1, x+2])
sample_rdd = rdd.sample(withReplacement=False, fraction=0.5)
distinct_rdd = rdd.distinct()
repartitioned_rdd = rdd.repartition(10)
coalesced_rdd = rdd.coalesce(5)  # Decrease partitions without shuffle
```

### Key RDD Actions
```python
# Actions (trigger computation)
count = rdd.count()
collected = rdd.collect()  # Returns all elements to driver
first_item = rdd.first()
top_items = rdd.top(5)  # Returns top 5 elements
take_items = rdd.take(5)  # Returns first 5 elements
as_dict = rdd.collectAsMap()  # Convert key-value pairs to dictionary
save_text = rdd.saveAsTextFile("path/to/output")
```

### RDD Pair Transformations (Key-Value RDDs)
```python
# Create key-value RDD
kv_rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 3)])

# GroupByKey
grouped_rdd = kv_rdd.groupByKey()
# Result: [('a', [1, 3]), ('b', [2])]

# ReduceByKey
reduced_rdd = kv_rdd.reduceByKey(lambda a, b: a + b)
# Result: [('a', 4), ('b', 2)]

# AggregateByKey
# (zero value, seq function, comb function)
agg_rdd = kv_rdd.aggregateByKey(0, lambda a, b: max(a, b), lambda a, b: a + b)

# Join operations
other_rdd = sc.parallelize([("a", "A"), ("b", "B"), ("c", "C")])
joined_rdd = kv_rdd.join(other_rdd)
# Result: [('a', (1, 'A')), ('a', (3, 'A')), ('b', (2, 'B'))]

left_joined = kv_rdd.leftOuterJoin(other_rdd)
right_joined = kv_rdd.rightOuterJoin(other_rdd)
```

## SQL Functions

### Importing Functions
```python
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, expr
```

### Aggregate Functions
```python
# Basic aggregations
df.select(
    F.count("*"),
    F.countDistinct("column"),
    F.sum("column"),
    F.avg("column"),
    F.mean("column"),
    F.min("column"),
    F.max("column")
).show()

# Statistical functions
df.select(
    F.stddev("column"),
    F.stddev_pop("column"),
    F.stddev_samp("column"),
    F.variance("column"),
    F.var_pop("column"),
    F.var_samp("column"),
    F.skewness("column"),
    F.kurtosis("column"),
    F.corr("col1", "col2"),
    F.covar_pop("col1", "col2"),
    F.covar_samp("col1", "col2")
).show()

# Collect list/set
df.groupBy("department").agg(
    F.collect_list("name").alias("all_names"),
    F.collect_set("name").alias("unique_names")
).show()
```

### String Functions
```python
# Common string operations
df.select(
    F.upper("name"),
    F.lower("name"),
    F.initcap("name"),    # Capitalize first letter of each word
    F.concat("fname", "lname"),
    F.concat_ws(" ", "fname", "lname"),  # With separator
    F.length("name"),
    F.trim("name"),
    F.ltrim("name"),
    F.rtrim("name"),
    F.lpad("name", 10, " "),  # Left pad to length 10
    F.rpad("name", 10, " "),  # Right pad to length 10
    F.substring("name", 1, 3)  # Extract substring (start, length)
).show()

# Pattern matching
df.select(
    F.regexp_replace("text", "pattern", "replacement"),
    F.regexp_extract("text", "(\\d+)", 1),  # Extract pattern matches
    F.translate("text", "abc", "123")  # Replace chars in 1st with chars in 2nd
).filter(F.col("text").rlike("pattern")).show()
```

### Date and Time Functions
```python
# Date/Time operations
df.select(
    F.current_date(),
    F.current_timestamp(),
    F.date_format("date_col", "yyyy-MM-dd"),
    F.to_date("date_str", "yyyy-MM-dd"),
    F.to_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss"),
    F.from_unixtime("unix_col"),
    F.unix_timestamp("ts_col"),
    F.year("date"),
    F.month("date"),
    F.dayofmonth("date"),
    F.hour("timestamp"),
    F.minute("timestamp"),
    F.second("timestamp"),
    F.weekofyear("date"),
    F.dayofweek("date"),  # Sunday = 1, Saturday = 7
    F.dayofyear("date"),
    F.quarter("date"),
    F.last_day("date"),   # Last day of the month
    F.datediff("end_date", "start_date"),
    F.months_between("end_date", "start_date"),
    F.add_months("date", 3),
    F.date_add("date", 10),
    F.date_sub("date", 10)
).show()
```

### Mathematical Functions
```python
# Basic math
df.select(
    F.abs("value"),
    F.sqrt("value"),
    F.cbrt("value"),      # Cube root
    F.exp("value"),       # e^value
    F.log("value"),       # Natural log
    F.log10("value"),     # Log base 10
    F.log2("value"),      # Log base 2
    F.pow("value", 2),    # value^2
    F.round("value", 2),  # Round to 2 decimals
    F.floor("value"),
    F.ceil("value"),
    F.sin("value"),
    F.cos("value"),
    F.tan("value"),
    F.asin("value"),
    F.acos("value"),
    F.atan("value"),
    F.sinh("value"),
    F.cosh("value"),
    F.tanh("value")
).show()
```

### Array/List Functions
```python
# Create and manipulate arrays
df.select(
    F.array("col1", "col2").alias("arr"),  # Create array from columns
    F.array_contains(F.col("arr"), "value"),  # Check if value is in array
    F.explode("arr"),     # Create one row per array element
    F.posexplode("arr"),  # With position
    F.array_join("arr", ", "),  # Join array elements to string
    F.array_max("arr"),   # Get max value in array
    F.array_min("arr"),   # Get min value in array
    F.array_position("arr", "value"),  # Position of first occurrence (1-based)
    F.element_at("arr", 2),  # Get element at position
    F.slice("arr", 1, 3),  # Get slice (start, length)
    F.sort_array("arr"),  # Sort array
    F.sort_array("arr", asc=False),  # Sort descending
    F.array_distinct("arr"),  # Remove duplicates
    F.size("arr")  # Array length
).show()
```

### Map Functions
```python
# Create and manipulate maps
df.select(
    F.create_map("key1", "value1", "key2", "value2").alias("map"),
    F.map_keys("map"),    # Get all keys
    F.map_values("map"),  # Get all values
    F.explode_outer("map"),  # Create one row per map entry
    F.map_from_entries("arr_of_structs"),  # Create map from array of structs
    F.map_entries("map"),    # Array of struct(key, value)
    F.map_concat("map1", "map2")  # Combine maps
).show()

# Access map values
df.select(F.col("map").getItem("key")).show()
```

### JSON Functions
```python
# Parse and manipulate JSON
df.select(
    F.from_json("json_col", schema).alias("parsed"),
    F.to_json("struct_col"),
    F.json_tuple("json_col", "field1", "field2"),
    F.get_json_object("json_col", "$.field.nested")
).show()
```

### UDFs (User Defined Functions)
```python
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

# Define UDF
def square(x):
    if x is None:
        return None
    return x * x

# Register UDF
square_udf = udf(square, IntegerType())

# Apply UDF
df.select("num", square_udf("num").alias("num_squared")).show()

# Alternative registration with decorators
@udf(IntegerType())
def cube(x):
    if x is None:
        return None
    return x * x * x

df.select("num", cube("num").alias("num_cubed")).show()

# Register UDF for SQL
spark.udf.register("sql_square", square, IntegerType())
df.createOrReplaceTempView("numbers")
spark.sql("SELECT num, sql_square(num) as squared FROM numbers").show()
```

### Pandas UDFs (Vectorized UDFs)
```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

# Scalar Pandas UDF
@pandas_udf("double")
def pandas_square(s: pd.Series) -> pd.Series:
    return s * s

df.select("num", pandas_square("num").alias("num_squared")).show()

# Grouped Map Pandas UDF
@pandas_udf("id long, value double")
def grouped_map_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf['value'] = pdf['value'] * 2
    return pdf

df.groupBy("id").apply(grouped_map_udf).show()

# Grouped Aggregate Pandas UDF
@pandas_udf("double")
def grouped_agg_udf(v: pd.Series) -> float:
    return v.mean() * 2

df.groupBy("id").agg(grouped_agg_udf("value").alias("doubled_mean")).show()
```

## Window Functions

### Creating a Window Specification
```python
from pyspark.sql import Window
import pyspark.sql.functions as F

# Define window
window_spec = Window.partitionBy("department").orderBy("salary")

# Without partition
window_spec = Window.orderBy("salary")

# With frame
window_spec = Window.partitionBy("department") \
                   .orderBy("salary") \
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow)
```

### Rank Functions
```python
# Ranking functions
df.select(
    "name", "department", "salary",
    F.rank().over(window_spec).alias("rank"),
    F.dense_rank().over(window_spec).alias("dense_rank"),
    F.row_number().over(window_spec).alias("row_number"),
    F.percent_rank().over(window_spec).alias("percent_rank"),
    F.ntile(4).over(window_spec).alias("quartile")
).show()
```

### Analytical Functions
```python
# Analytical functions
df.select(
    "name", "department", "salary",
    F.lag("salary", 1).over(window_spec).alias("prev_salary"),
    F.lead("salary", 1).over(window_spec).alias("next_salary"),
    F.first("salary").over(window_spec).alias("first_salary"),
    F.last("salary").over(window_spec).alias("last_salary"),
    F.sum("salary").over(window_spec).alias("running_sum"),
    F.avg("salary").over(window_spec).alias("running_avg"),
    F.min("salary").over(window_spec).alias("running_min"),
    F.max("salary").over(window_spec).alias("running_max"),
    F.count("salary").over(window_spec).alias("running_count")
).show()
```

## Machine Learning (MLlib)

### Data Preparation
```python
from pyspark.ml.feature import *

# StringIndexer - Convert string labels to indices
indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
indexed_df = indexer.fit(df).transform(df)

# OneHotEncoder - One-hot encode categorical features
encoder = OneHotEncoder(inputCol="categoryIndex", outputCol="categoryVec")
encoded_df = encoder.fit(indexed_df).transform(indexed_df)

# VectorAssembler - Combine features into feature vector
assembler = VectorAssembler(
    inputCols=["feature1", "feature2", "categoryVec"], 
    outputCol="features"
)
assembled_df = assembler.transform(encoded_df)

# StandardScaler - Normalize features
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
scaled_df = scaler.fit(assembled_df).transform(assembled_df)

# MinMaxScaler - Scale features to [0,1] range
minmax = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
rescaled_df = minmax.fit(assembled_df).transform(assembled_df)

# PCA - Dimensionality reduction
pca = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
pca_df = pca.fit(assembled_df).transform(assembled_df)

# Tokenizer - Split text into words
tokenizer = Tokenizer(inputCol="text", outputCol="words")
tokenized_df = tokenizer.transform(df)

# StopWordsRemover - Remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
filtered_df = remover.transform(tokenized_df)

# NGram - Create n-grams from words
ngram = NGram(n=2, inputCol="words", outputCol="bigrams")
ngram_df = ngram.transform(tokenized_df)

# HashingTF - Convert text to feature vectors
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurized_df = hashingTF.transform(tokenized_df)

# IDF - Compute Inverse Document Frequency
idf = IDF(inputCol="rawFeatures", outputCol="features")
idf_model = idf.fit(featurized_df)
tfidf_df = idf_model.transform(featurized_df)

# Word2Vec - Convert words to vectors
word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="words", outputCol="result")
w2v_model = word2Vec.fit(tokenized_df)
w2v_df = w2v_model.transform(tokenized_df)
```

### Creating ML Pipeline
```python
from pyspark.ml import Pipeline

# Create a pipeline
pipeline = Pipeline(stages=[
    tokenizer,
    remover,
    hashingTF,
    idf,
    assembler,
    scaler
])

# Fit and transform with pipeline
pipeline_model = pipeline.fit(df)
pipeline_df = pipeline_model.transform(df)

# Save and load pipeline model
pipeline_model.save("path/to/pipeline-model")
from pyspark.ml import PipelineModel
loaded_model = PipelineModel.load("path/to/pipeline-model")
```

### Classification Models
```python
from pyspark.ml.classification import *

# Split data
train_data, test_data = assembled_df.randomSplit([0.8, 0.2], seed=42)

# Logistic Regression
lr = LogisticRegression(featuresCol="features", labelCol="label")
lr_model = lr.fit(train_data)
lr_predictions = lr_model.transform(test_data)

# Decision Tree Classifier
dt = DecisionTreeClassifier(featuresCol="features", labelCol="label")
dt_model = dt.fit(train_data)
dt_predictions = dt_model.transform(test_data)

# Random Forest Classifier
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=10)
rf_model = rf.fit(train_data)
rf_predictions = rf_model.transform(test_data)

# Gradient-Boosted Tree Classifier
gbt = GBTClassifier(featuresCol="features", labelCol="label", maxIter=10)
gbt_model = gbt.fit(train_data)
gbt_predictions = gbt_model.transform(test_data)

# Naive Bayes
nb = NaiveBayes(featuresCol="features", labelCol="label")
nb_model = nb.fit(train_data)
nb_predictions = nb_model.transform(test_data)

# Support Vector Machine
svm = LinearSVC(featuresCol="features", labelCol="label")
svm_model = svm.fit(train_data)
svm_predictions = svm_model.transform(test_data)

# Multilayer Perceptron
layers = [len(features), 10, 5, 2]  # input, hidden layers, output
mlp = MultilayerPerceptronClassifier(
    featuresCol="features", labelCol="label", layers=layers)
mlp_model = mlp.fit(train_data)
mlp_predictions = mlp_model.transform(test_data)
```

### Regression Models
```python
from pyspark.ml.regression import *

# Linear Regression
lr = LinearRegression(featuresCol="features", labelCol="label")
lr_model = lr.fit(train_data)
lr_predictions = lr_model.transform(test_data)

# Decision Tree Regressor
dt = DecisionTreeRegressor(featuresCol="features", labelCol="label")
dt_model = dt.fit(train_data)
dt_predictions = dt_model.transform(test_data)

# Random Forest Regressor
rf = RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=10)
rf_model = rf.fit(train_data)
rf_predictions = rf_model.transform(test_data)

# Gradient-Boosted Tree Regressor
gbt = GBTRegressor(featuresCol="features", labelCol="label", maxIter=10)
gbt_model = gbt.fit(train_data)
gbt_predictions = gbt_model.transform(test_data)

# Generalized Linear Regression
glr = GeneralizedLinearRegression(
    family="gaussian", link="identity", 
    featuresCol="features", labelCol="label")
glr_model = glr.fit(train_data)
glr_predictions = glr_model.transform(test_data)

# Factorization Machines Regressor
fm = FMRegressor(featuresCol="features", labelCol="label")
fm_model = fm.fit(train_data)
fm_predictions = fm_model.transform(test_data)
```

### Clustering Models
```python
from pyspark.ml.clustering import *

# K-Means clustering
kmeans = KMeans(featuresCol="features", k=3)
kmeans_model = kmeans.fit(df)
kmeans_predictions = kmeans_model.transform(df)

# Gaussian Mixture Model
gmm = GaussianMixture(featuresCol="features", k=3)
gmm_model = gmm.fit(df)
gmm_predictions = gmm_model.transform(df)

# Bisecting K-Means
bkm = BisectingKMeans(featuresCol="features", k=3)
bkm_model = bkm.fit(df)
bkm_predictions = bkm_model.transform(df)

# Latent Dirichlet Allocation
lda = LDA(featuresCol="features", k=10)
lda_model = lda.fit(df)
lda_predictions = lda_model.transform(df)
```

### Model Evaluation
```python
from pyspark.ml.evaluation import *

# Binary Classification Evaluator
evaluator = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)

# Multiclass Classification Evaluator
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
# Other metrics: "f1", "weightedPrecision", "weightedRecall"

# Regression Evaluator
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
# Other metrics: "mse", "r2", "mae"

# Clustering Evaluator
evaluator = ClusteringEvaluator(
    featuresCol="features", predictionCol="prediction")
silhouette = evaluator.evaluate(predictions)
```

### CrossValidator and TrainValidationSplit
```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, TrainValidationSplit

# Define parameter grid
param_grid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()
```


