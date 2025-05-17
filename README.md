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

# Cross validation
cv = CrossValidator(
    estimator=lr,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3
)
cv_model = cv.fit(train_data)
cv_predictions = cv_model.transform(test_data)
best_model = cv_model.bestModel

# Train validation split
tvs = TrainValidationSplit(
    estimator=lr,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    trainRatio=0.8
)
tvs_model = tvs.fit(train_data)
tvs_predictions = tvs_model.transform(test_data)
```

## Streaming

### Creating Streaming DataFrame
```python
# From socket source
socket_stream_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# From file source
file_stream_df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("path", "path/to/directory") \
    .option("maxFilesPerTrigger", 1) \
    .load()

# From Kafka source
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host:port") \
    .option("subscribe", "topic1,topic2") \
    .load()
```

### Processing Streaming Data
```python
# Select and process data
processed_df = stream_df.select(
    F.col("value").cast("string").alias("data")
).select(
    F.json_tuple("data", "name", "age").alias("name", "age")
)

# Aggregation with streaming data
agg_df = stream_df.groupBy(
    F.window(F.col("timestamp"), "10 minutes", "5 minutes"),
    F.col("category")
).count()
```

### Output Modes
```python
# Complete mode (output all results)
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Append mode (output only new results)
query = processed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "path/to/output") \
    .option("checkpointLocation", "path/to/checkpoint") \
    .start()

# Update mode (output only updated results)
query = agg_df.writeStream \
    .outputMode("update") \
    .format("memory") \
    .queryName("streaming_table") \
    .start()
```

### Streaming Output Sinks
```python
# Console sink (for debugging)
query = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# File sink
query = df.writeStream \
    .format("parquet")  # or "csv", "json", "orc"
    .option("path", "path/to/output") \
    .option("checkpointLocation", "path/to/checkpoint") \
    .outputMode("append") \
    .start()

# Kafka sink
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host:port") \
    .option("topic", "output_topic") \
    .option("checkpointLocation", "path/to/checkpoint") \
    .start()

# Memory sink (for interactive queries)
query = df.writeStream \
    .format("memory") \
    .queryName("table_name") \
    .outputMode("complete") \
    .start()

# Query the in-memory table
spark.sql("SELECT * FROM table_name").show()
```

### Managing Streaming Queries
```python
# Wait for query termination
query.awaitTermination()

# Stop query
query.stop()

# Get active queries
active_queries = spark.streams.active

# Get query status
query_status = query.status
query_is_active = query.isActive

# Handle streaming errors
try:
    query.awaitTermination()
except StreamingQueryException as e:
    print(f"Query failed: {e.message}")
```

## Performance Optimization

### Caching and Persistence
```python
# Cache data in memory
df.cache()
df.persist()

# Persist with different storage levels
from pyspark.storagelevel import StorageLevel
df.persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.MEMORY_AND_DISK)
df.persist(StorageLevel.MEMORY_ONLY_SER)
df.persist(StorageLevel.DISK_ONLY)
df.persist(StorageLevel.OFF_HEAP)

# Unpersist data
df.unpersist()
```

### Partition Management
```python
# Check current number of partitions
df.rdd.getNumPartitions()

# Repartition data (full shuffle)
df_repart = df.repartition(10)
df_repart = df.repartition("department")  # by column
df_repart = df.repartition(10, "department")  # by column with num partitions

# Coalesce data (minimizes shuffling, reduces partitions)
df_coalesce = df.coalesce(5)
```

### Broadcast Variables
```python
# Create broadcast variable
broadcast_var = sc.broadcast([1, 2, 3])

# Access broadcast variable
broadcast_var.value

# Using broadcast hint for small DataFrames in joins
from pyspark.sql.functions import broadcast
df_result = df_large.join(broadcast(df_small), "key")
```

### Accumulator Variables
```python
# Create accumulator
accum = sc.accumulator(0)

# Update in operations
def add_to_accum(x):
    global accum
    accum += x
    return x

rdd.foreach(add_to_accum)

# Get accumulator value
accum.value
```

### Execution Plan
```python
# Get execution plan
df.explain()  # Simplified plan
df.explain(True)  # Detailed plan
df.explain("formatted")  # Formatted plan
```

### Configuration Tuning
```python
# Memory management
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.5")

# Shuffle performance
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.default.parallelism", "200")

# Serialization
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryo.registrationRequired", "false")

# Dynamic allocation
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "1")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "10")

# Adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

## Snowflake Integration

### Setting Up Snowflake Connection
```python
# Create SparkSession with Snowflake support
spark = SparkSession.builder \
    .appName("Snowflake Integration") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.1") \
    .getOrCreate()

# Snowflake connection parameters
sf_options = {
    "sfURL": "account.snowflakecomputing.com",
    "sfUser": "username",
    "sfPassword": "password",
    "sfDatabase": "database",
    "sfSchema": "schema",
    "sfWarehouse": "warehouse",
    "sfRole": "role"
}

# Alternative with key authentication
sf_options_with_key = {
    "sfURL": "account.snowflakecomputing.com",
    "sfUser": "username",
    "pem_private_key": "path/to/private/key.p8",
    "sfDatabase": "database",
    "sfSchema": "schema",
    "sfWarehouse": "warehouse",
    "sfRole": "role"
}
```

### Reading from Snowflake
```python
# Read from Snowflake table
df = spark.read \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("dbtable", "schema.table_name") \
    .load()

# Read with query
df = spark.read \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("query", "SELECT * FROM schema.table_name WHERE column = 'value'") \
    .load()

# Read with auto-pushdown
df = spark.read \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("dbtable", "schema.table_name") \
    .option("autopushdown", "true") \
    .load() \
    .filter(F.col("date") >= "2023-01-01") \
    .select("id", "name", "value")
```

### Writing to Snowflake
```python
# Write to Snowflake table
df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("dbtable", "schema.table_name") \
    .mode("append") \
    .save()

# Write with additional options
df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("dbtable", "schema.table_name") \
    .option("truncate_table", "true") \
    .option("usestagingtable", "false") \
    .mode("overwrite") \
    .save()
```

### Performance Optimizations for Snowflake
```python
# Configure partition size for better parallelism
df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("dbtable", "schema.table_name") \
    .option("partition_size_in_mb", "128") \
    .option("parallelism", "8") \
    .mode("append") \
    .save()

# Use internal staging area
df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("dbtable", "schema.table_name") \
    .option("sfCompress", "true") \
    .option("use_internal_stage", "true") \
    .mode("append") \
    .save()

# Configure column mapping
df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("dbtable", "schema.table_name") \
    .option("column_mapping", "name") \
    .mode("append") \
    .save()
```

### Temporary Tables and Views
```python
# Create temporary view from Snowflake query
spark.read \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("query", "SELECT * FROM schema.table_name") \
    .load() \
    .createOrReplaceTempView("temp_view")

# Use in Spark SQL
result_df = spark.sql("SELECT * FROM temp_view WHERE value > 100")

# Write back to Snowflake
result_df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("dbtable", "schema.results_table") \
    .mode("overwrite") \
    .save()
```

# PySpark Functions Overview

This comprehensive reference table covers the most commonly used PySpark functions across different categories, along with examples and use cases.

## Table of Contents
- [DataFrame Creation and I/O](#dataframe-creation-and-io)
- [DataFrame Transformation](#dataframe-transformation)
- [Column Operations](#column-operations)
- [Aggregation Functions](#aggregation-functions)
- [Window Functions](#window-functions)
- [String Functions](#string-functions)
- [Date/Time Functions](#datetime-functions)
- [Mathematical Functions](#mathematical-functions)
- [Collection Functions](#collection-functions)
- [UDFs (User-Defined Functions)](#udfs-user-defined-functions)
- [ML Pipeline Components](#ml-pipeline-components)

## DataFrame Creation and I/O

| Function | Description | Example |
|----------|-------------|---------|
| `spark.read.format()` | Loads data from various file formats | `df = spark.read.format("csv").option("header", "true").load("data.csv")` |
| `spark.createDataFrame()` | Creates a DataFrame from Python objects | `df = spark.createDataFrame([("John", 30), ("Alice", 25)], ["name", "age"])` |
| `df.write.format()` | Writes DataFrame to various formats | `df.write.format("parquet").save("output.parquet")` |
| `spark.sql()` | Executes SQL queries | `df = spark.sql("SELECT * FROM employees WHERE age > 30")` |
| `spark.table()` | References a table in the catalog | `df = spark.table("employees")` |
| `df.createOrReplaceTempView()` | Creates a temp view of DataFrame | `df.createOrReplaceTempView("employees")` |
| `spark.read.json()` | Reads JSON files | `df = spark.read.json("data.json")` |
| `spark.read.parquet()` | Reads Parquet files | `df = spark.read.parquet("data.parquet")` |

## DataFrame Transformation

| Function | Description | Example |
|----------|-------------|---------|
| `df.select()` | Selects columns from DataFrame | `df.select("name", "age")` |
| `df.filter()` | Filters rows based on condition | `df.filter(df.age > 30)` |
| `df.where()` | Alias for filter | `df.where("age > 30")` |
| `df.groupBy()` | Groups DataFrame by columns | `df.groupBy("department").count()` |
| `df.join()` | Joins two DataFrames | `df1.join(df2, df1.id == df2.id, "inner")` |
| `df.unionByName()` | Combines rows with matching columns | `df1.unionByName(df2, allowMissingColumns=True)` |
| `df.withColumn()` | Adds or replaces a column | `df.withColumn("doubled_salary", df.salary * 2)` |
| `df.withColumnRenamed()` | Renames a column | `df.withColumnRenamed("salary", "annual_pay")` |
| `df.drop()` | Drops a column | `df.drop("temp_column")` |
| `df.distinct()` | Returns distinct rows | `df.select("department").distinct()` |
| `df.sort()` | Sorts DataFrame by columns | `df.sort(df.age.desc(), "name")` |
| `df.orderBy()` | Alias for sort | `df.orderBy(["age", "name"], ascending=[False, True])` |
| `df.limit()` | Limits number of rows | `df.limit(10)` | 
| `df.sample()` | Samples fraction of rows | `df.sample(fraction=0.1, seed=42)` |
| `df.randomSplit()` | Splits into multiple DFs | `train, test = df.randomSplit([0.8, 0.2], seed=42)` |
| `df.describe()` | Basic statistics of numeric columns | `df.describe().show()` |
| `df.dropDuplicates()` | Removes duplicate rows | `df.dropDuplicates(["name", "department"])` |
| `df.dropna()` | Drops rows with null values | `df.dropna(subset=["name", "age"])` |
| `df.fillna()` | Replaces null values | `df.fillna({"age": 0, "name": "Unknown"})` |
| `df.replace()` | Replaces values in columns | `df.replace([0], [None], subset=["age"])` |
| `df.crosstab()` | Creates contingency table | `df.crosstab("gender", "department")` |
| `df.cube()` | Creates cube for aggregation | `df.cube("department", "gender").count()` |
| `df.rollup()` | Creates rollup for aggregation | `df.rollup("department", "gender").count()` |
| `df.pivot()` | Pivots DataFrame | `df.groupBy("date").pivot("category").count()` |
| `df.agg()` | Performs aggregations | `df.groupBy("department").agg({"salary": "avg", "age": "max"})` |

## Column Operations

| Function | Description | Example |
|----------|-------------|---------|
| `col()` | References a column | `from pyspark.sql.functions import col; df.select(col("name"))` |
| `expr()` | Evaluates SQL expression | `from pyspark.sql.functions import expr; df.select(expr("salary * 1.1"))` |
| `lit()` | Creates a column with literal value | `from pyspark.sql.functions import lit; df.select(df.name, lit(1).alias("constant"))` |
| `alias()` | Renames column | `df.select(df.name.alias("full_name"))` |
| `isNull()` | Checks if column is null | `df.filter(df.name.isNull())` |
| `isNotNull()` | Checks if column is not null | `df.filter(df.name.isNotNull())` |
| `isin()` | Checks if value is in list | `df.filter(df.department.isin("HR", "Sales"))` |
| `cast()` | Converts column type | `df.select(df.age.cast("string"))` |
| `between()` | Checks if value is in range | `df.filter(df.age.between(20, 30))` |
| `when()` | Conditional expressions | `from pyspark.sql.functions import when; df.select(when(df.age > 30, "Senior").otherwise("Junior").alias("level"))` |
| `otherwise()` | Complement to when | *(see when example above)* |
| `asc()` | Sorts ascending | `df.sort(df.age.asc())` |
| `desc()` | Sorts descending | `df.sort(df.age.desc())` |
| `startswith()` | Checks string prefix | `df.filter(df.name.startswith("A"))` |
| `endswith()` | Checks string suffix | `df.filter(df.name.endswith("son"))` |
| `contains()` | Checks substring presence | `df.filter(df.name.contains("John"))` |
| `like()` | SQL LIKE pattern matching | `df.filter(df.name.like("%John%"))` |
| `rlike()` | Regex pattern matching | `df.filter(df.name.rlike("^[A-Z].*"))` |

## Aggregation Functions

| Function | Description | Example |
|----------|-------------|---------|
| `count()` | Counts rows | `from pyspark.sql.functions import count; df.select(count("*"))` |
| `countDistinct()` | Counts distinct values | `from pyspark.sql.functions import countDistinct; df.select(countDistinct("department"))` |
| `approx_count_distinct()` | Approximates distinct count | `from pyspark.sql.functions import approx_count_distinct; df.select(approx_count_distinct("email", 0.05))` |
| `sum()` | Calculates sum | `from pyspark.sql.functions import sum; df.groupBy("department").agg(sum("salary"))` |
| `avg()` | Calculates average | `from pyspark.sql.functions import avg; df.groupBy("department").agg(avg("salary"))` |
| `mean()` | Alias for avg | `from pyspark.sql.functions import mean; df.groupBy("department").agg(mean("salary"))` |
| `min()` | Finds minimum value | `from pyspark.sql.functions import min; df.groupBy("department").agg(min("salary"))` |
| `max()` | Finds maximum value | `from pyspark.sql.functions import max; df.groupBy("department").agg(max("salary"))` |
| `first()` | Gets first value | `from pyspark.sql.functions import first; df.groupBy("department").agg(first("name"))` |
| `last()` | Gets last value | `from pyspark.sql.functions import last; df.groupBy("department").agg(last("name"))` |
| `corr()` | Calculates correlation | `from pyspark.sql.functions import corr; df.select(corr("height", "weight"))` |
| `covar_pop()` | Population covariance | `from pyspark.sql.functions import covar_pop; df.select(covar_pop("height", "weight"))` |
| `covar_samp()` | Sample covariance | `from pyspark.sql.functions import covar_samp; df.select(covar_samp("height", "weight"))` |
| `kurtosis()` | Calculates kurtosis | `from pyspark.sql.functions import kurtosis; df.select(kurtosis("height"))` |
| `skewness()` | Calculates skewness | `from pyspark.sql.functions import skewness; df.select(skewness("height"))` |
| `stddev()` | Calculates standard deviation | `from pyspark.sql.functions import stddev; df.select(stddev("salary"))` |
| `stddev_pop()` | Population std deviation | `from pyspark.sql.functions import stddev_pop; df.select(stddev_pop("salary"))` |
| `stddev_samp()` | Sample std deviation | `from pyspark.sql.functions import stddev_samp; df.select(stddev_samp("salary"))` |
| `variance()` | Calculates variance | `from pyspark.sql.functions import variance; df.select(variance("salary"))` |
| `var_pop()` | Population variance | `from pyspark.sql.functions import var_pop; df.select(var_pop("salary"))` |
| `var_samp()` | Sample variance | `from pyspark.sql.functions import var_samp; df.select(var_samp("salary"))` |
| `collect_list()` | Collects values into list | `from pyspark.sql.functions import collect_list; df.groupBy("department").agg(collect_list("name"))` |
| `collect_set()` | Collects unique values into set | `from pyspark.sql.functions import collect_set; df.groupBy("department").agg(collect_set("name"))` |

## Window Functions

| Function | Description | Example |
|----------|-------------|---------|
| `Window.partitionBy()` | Defines window partitioning | `from pyspark.sql.window import Window; window = Window.partitionBy("department")` |
| `Window.orderBy()` | Defines window ordering | `window = Window.partitionBy("department").orderBy("salary")` |
| `Window.rowsBetween()` | Defines row frame | `window = Window.orderBy("date").rowsBetween(-2, 0)` |
| `Window.rangeBetween()` | Defines range frame | `window = Window.orderBy("date").rangeBetween(-2, 0)` |
| `rank()` | Assigns rank | `from pyspark.sql.functions import rank; df.withColumn("rank", rank().over(window))` |
| `dense_rank()` | Assigns dense rank | `from pyspark.sql.functions import dense_rank; df.withColumn("dense_rank", dense_rank().over(window))` |
| `row_number()` | Assigns row number | `from pyspark.sql.functions import row_number; df.withColumn("row_number", row_number().over(window))` |
| `ntile()` | Assigns ntile | `from pyspark.sql.functions import ntile; df.withColumn("quartile", ntile(4).over(window))` |
| `cume_dist()` | Calculates cumulative distribution | `from pyspark.sql.functions import cume_dist; df.withColumn("cume_dist", cume_dist().over(window))` |
| `percent_rank()` | Calculates percent rank | `from pyspark.sql.functions import percent_rank; df.withColumn("percent_rank", percent_rank().over(window))` |
| `lag()` | Accesses previous row value | `from pyspark.sql.functions import lag; df.withColumn("prev_salary", lag("salary", 1).over(window))` |
| `lead()` | Accesses next row value | `from pyspark.sql.functions import lead; df.withColumn("next_salary", lead("salary", 1).over(window))` |

## String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `concat()` | Concatenates strings | `from pyspark.sql.functions import concat; df.select(concat(df.first_name, lit(" "), df.last_name))` |
| `concat_ws()` | Concatenates with separator | `from pyspark.sql.functions import concat_ws; df.select(concat_ws(" ", df.first_name, df.last_name))` |
| `upper()` | Converts to uppercase | `from pyspark.sql.functions import upper; df.select(upper(df.name))` |
| `lower()` | Converts to lowercase | `from pyspark.sql.functions import lower; df.select(lower(df.name))` |
| `trim()` | Removes whitespace | `from pyspark.sql.functions import trim; df.select(trim(df.name))` |
| `ltrim()` | Removes leading whitespace | `from pyspark.sql.functions import ltrim; df.select(ltrim(df.name))` |
| `rtrim()` | Removes trailing whitespace | `from pyspark.sql.functions import rtrim; df.select(rtrim(df.name))` |
| `lpad()` | Left pads string | `from pyspark.sql.functions import lpad; df.select(lpad(df.id, 5, "0"))` |
| `rpad()` | Right pads string | `from pyspark.sql.functions import rpad; df.select(rpad(df.id, 5, "0"))` |
| `length()` | Returns string length | `from pyspark.sql.functions import length; df.select(length(df.name))` |
| `regexp_replace()` | Replaces regex matches | `from pyspark.sql.functions import regexp_replace; df.select(regexp_replace(df.phone, "[()-]", ""))` |
| `regexp_extract()` | Extracts regex matches | `from pyspark.sql.functions import regexp_extract; df.select(regexp_extract(df.email, "([^@]+)", 1))` |
| `split()` | Splits string into array | `from pyspark.sql.functions import split; df.select(split(df.tags, ","))` |
| `substring()` | Extracts substring | `from pyspark.sql.functions import substring; df.select(substring(df.name, 1, 3))` |
| `initcap()` | Capitalizes first letter | `from pyspark.sql.functions import initcap; df.select(initcap(df.name))` |
| `soundex()` | Calculates soundex code | `from pyspark.sql.functions import soundex; df.select(soundex(df.name))` |
| `levenshtein()` | Calculates edit distance | `from pyspark.sql.functions import levenshtein; df.select(levenshtein(df.name1, df.name2))` |
| `translate()` | Replaces characters | `from pyspark.sql.functions import translate; df.select(translate(df.name, "AEIOU", "12345"))` |

## Date/Time Functions

| Function | Description | Example |
|----------|-------------|---------|
| `current_date()` | Gets current date | `from pyspark.sql.functions import current_date; df.withColumn("today", current_date())` |
| `current_timestamp()` | Gets current timestamp | `from pyspark.sql.functions import current_timestamp; df.withColumn("now", current_timestamp())` |
| `date_format()` | Formats date | `from pyspark.sql.functions import date_format; df.select(date_format(df.date, "yyyy-MM-dd"))` |
| `to_date()` | Converts to date | `from pyspark.sql.functions import to_date; df.withColumn("date", to_date(df.date_str, "yyyy-MM-dd"))` |
| `to_timestamp()` | Converts to timestamp | `from pyspark.sql.functions import to_timestamp; df.withColumn("ts", to_timestamp(df.ts_str, "yyyy-MM-dd HH:mm:ss"))` |
| `from_unixtime()` | Converts unix timestamp | `from pyspark.sql.functions import from_unixtime; df.select(from_unixtime(df.epoch))` |
| `unix_timestamp()` | Converts to unix timestamp | `from pyspark.sql.functions import unix_timestamp; df.select(unix_timestamp(df.date))` |
| `datediff()` | Calculates date difference | `from pyspark.sql.functions import datediff; df.select(datediff(df.end_date, df.start_date))` |
| `add_months()` | Adds months to date | `from pyspark.sql.functions import add_months; df.select(add_months(df.date, 3))` |
| `months_between()` | Calculates months between | `from pyspark.sql.functions import months_between; df.select(months_between(df.end_date, df.start_date))` |
| `date_add()` | Adds days to date | `from pyspark.sql.functions import date_add; df.select(date_add(df.date, 7))` |
| `date_sub()` | Subtracts days from date | `from pyspark.sql.functions import date_sub; df.select(date_sub(df.date, 7))` |
| `year()` | Extracts year | `from pyspark.sql.functions import year; df.select(year(df.date))` |
| `month()` | Extracts month | `from pyspark.sql.functions import month; df.select(month(df.date))` |
| `dayofmonth()` | Extracts day of month | `from pyspark.sql.functions import dayofmonth; df.select(dayofmonth(df.date))` |
| `dayofweek()` | Extracts day of week | `from pyspark.sql.functions import dayofweek; df.select(dayofweek(df.date))` |
| `dayofyear()` | Extracts day of year | `from pyspark.sql.functions import dayofyear; df.select(dayofyear(df.date))` |
| `hour()` | Extracts hour | `from pyspark.sql.functions import hour; df.select(hour(df.timestamp))` |
| `minute()` | Extracts minute | `from pyspark.sql.functions import minute; df.select(minute(df.timestamp))` |
| `second()` | Extracts second | `from pyspark.sql.functions import second; df.select(second(df.timestamp))` |
| `weekofyear()` | Extracts week of year | `from pyspark.sql.functions import weekofyear; df.select(weekofyear(df.date))` |
| `last_day()` | Gets last day of month | `from pyspark.sql.functions import last_day; df.select(last_day(df.date))` |
| `next_day()` | Gets next day of week | `from pyspark.sql.functions import next_day; df.select(next_day(df.date, "Sunday"))` |
| `quarter()` | Extracts quarter | `from pyspark.sql.functions import quarter; df.select(quarter(df.date))` |
| `trunc()` | Truncates date | `from pyspark.sql.functions import trunc; df.select(trunc(df.date, "month"))` |

## Mathematical Functions

| Function | Description | Example |
|----------|-------------|---------|
| `abs()` | Absolute value | `from pyspark.sql.functions import abs; df.select(abs(df.value))` |
| `sqrt()` | Square root | `from pyspark.sql.functions import sqrt; df.select(sqrt(df.value))` |
| `cbrt()` | Cube root | `from pyspark.sql.functions import cbrt; df.select(cbrt(df.value))` |
| `exp()` | Exponential | `from pyspark.sql.functions import exp; df.select(exp(df.value))` |
| `log()` | Natural logarithm | `from pyspark.sql.functions import log; df.select(log(df.value))` |
| `log10()` | Base-10 logarithm | `from pyspark.sql.functions import log10; df.select(log10(df.value))` |
| `log2()` | Base-2 logarithm | `from pyspark.sql.functions import log2; df.select(log2(df.value))` |
| `pow()` | Power | `from pyspark.sql.functions import pow; df.select(pow(df.value, 2))` |
| `round()` | Rounds number | `from pyspark.sql.functions import round; df.select(round(df.value, 2))` |
| `ceil()` | Ceiling | `from pyspark.sql.functions import ceil; df.select(ceil(df.value))` |
| `floor()` | Floor | `from pyspark.sql.functions import floor; df.select(floor(df.value))` |
| `signum()` | Sign | `from pyspark.sql.functions import signum; df.select(signum(df.value))` |
| `sin()` | Sine | `from pyspark.sql.functions import sin; df.select(sin(df.angle))` |
| `cos()` | Cosine | `from pyspark.sql.functions import cos; df.select(cos(df.angle))` |
| `tan()` | Tangent | `from pyspark.sql.functions import tan; df.select(tan(df.angle))` |
| `asin()` | Arcsine | `from pyspark.sql.functions import asin; df.select(asin(df.value))` |
| `acos()` | Arccosine | `from pyspark.sql.functions import acos; df.select(acos(df.value))` |
| `atan()` | Arctangent | `from pyspark.sql.functions import atan; df.select(atan(df.value))` |
| `degrees()` | Radians to degrees | `from pyspark.sql.functions import degrees; df.select(degrees(df.radians))` |
| `radians()` | Degrees to radians | `from pyspark.sql.functions import radians; df.select(radians(df.degrees))` |
| `rand()` | Random number | `from pyspark.sql.functions import rand; df.select(rand())` |
| `randn()` | Random normal | `from pyspark.sql.functions import randn; df.select(randn())` |

## Collection Functions

| Function | Description | Example |
|----------|-------------|---------|
| `array()` | Creates array | `from pyspark.sql.functions import array; df.select(array(df.col1, df.col2, df.col3))` |
| `array_contains()` | Checks if array contains value | `from pyspark.sql.functions import array_contains; df.filter(array_contains(df.tags, "python"))` |
| `array_distinct()` | Removes duplicates from array | `from pyspark.sql.functions import array_distinct; df.select(array_distinct(df.tags))` |
| `array_intersect()` | Intersects arrays | `from pyspark.sql.functions import array_intersect; df.select(array_intersect(df.tags1, df.tags2))` |
| `array_union()` | Unions arrays | `from pyspark.sql.functions import array_union; df.select(array_union(df.tags1, df.tags2))` |
| `array_except()` | Array difference | `from pyspark.sql.functions import array_except; df.select(array_except(df.tags1, df.tags2))` |
| `array_join()` | Joins array elements | `from pyspark.sql.functions import array_join; df.select(array_join(df.tags, ", "))` |
| `array_max()` | Gets max value in array | `from pyspark.sql.functions import array_max; df.select(array_max(df.values))` |
| `array_min()` | Gets min value in array | `from pyspark.sql.functions import array_min; df.select(array_min(df.values))` |
| `array_position()` | Gets element position | `from pyspark.sql.functions import array_position; df.select(array_position(df.tags, "python"))` |
| `array_remove()` | Removes element from array | `from pyspark.sql.functions import array_remove; df.select(array_remove(df.tags, "java"))` |
| `array_sort()` | Sorts array | `from pyspark.sql.functions import array_sort; df.select(array_sort(df.values))` |
| `arrays_overlap()` | Checks if arrays overlap | `from pyspark.sql.functions import arrays_overlap; df.filter(arrays_overlap(df.tags1, df.tags2))` |
| `arrays_zip()` | Zips arrays | `from pyspark.sql.functions import arrays_zip; df.select(arrays_zip(df.keys, df.values))` |
| `element_at()` | Gets element at position | `from pyspark.sql.functions import element_at; df.select(element_at(df.tags, 1))` |
| `explode()` | Explodes array to rows | `from pyspark.sql.functions import explode; df.select(df.name, explode(df.tags))` |
| `explode_outer()` | Explodes with nulls | `from pyspark.sql.functions import explode_outer; df.select(df.name, explode_outer(df.tags))` |
| `posexplode()` | Explodes with position | `from pyspark.sql.functions import posexplode; df.select(df.name, posexplode(df.tags))` |
| `posexplode_outer()` | Explodes with position and nulls | `from pyspark.sql.functions import posexplode_outer; df.select(df.name, posexplode_outer(df.tags))` |
| `size()` | Gets array size | `from pyspark.sql.functions import size; df.select(size(df.tags))` |
| `slice()` | Gets array slice | `from pyspark.sql.functions import slice; df.select(slice(df.tags, 1, 2))` |
| `sort_array()` | Sorts array | `from pyspark.sql.functions import sort_array; df.select(sort_array(df.values))` |
| `map_keys()` | Gets map keys | `from pyspark.sql.functions import map_keys; df.select(map_keys(df.properties))` |
| `map_values()` | Gets map values | `from pyspark.sql.functions import map_values; df.select(map_values(df.properties))` |
| `map_entries()` | Gets map entries | `from pyspark.sql.functions import map_entries; df.select(map_entries(df.properties))` |
| `map_from_entries()` | Creates map from entries | `from pyspark.sql.functions import map_from_entries; df.select(map_from_entries(df.entries))` |
| `map_concat()` | Concatenates maps | `from pyspark.sql.functions import map_concat; df.select(map_concat(df.map1, df.map2))` |

## UDFs (User-Defined Functions)

| Function | Description | Example |
|----------|-------------|---------|
| `udf()` | Creates a UDF | ```from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
square = udf(lambda x: x * x, IntegerType())
df.select(square(df.value))``` |
| `pandas_udf()` | Creates a Pandas UDF | ```from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType
import pandas as pd

@pandas_udf(IntegerType())
def square(x: pd.Series) -> pd.Series:
    return x * x
    
df.select(square(df.value))``` |

## ML Pipeline Components

| Component | Description | Example |
|-----------|-------------|---------|
| `VectorAssembler` | Combines features into vector | ```from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=["age", "height", "weight"], outputCol="features")
assembled_df = assembler.transform(df)``` |
| `StringIndexer` | Encodes string labels | ```from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
indexed_df = indexer.fit(df).transform(df)``` |
| `OneHotEncoder` | One-hot encodes categorical features | ```from pyspark.ml.feature import OneHotEncoder
encoder = OneHotEncoder(inputCols=["categoryIndex"], outputCols=["categoryVec"])
encoded_df = encoder.fit(indexed_df).transform(indexed_df)``` |
| `StandardScaler` | Standardizes features | ```from pyspark.ml.feature import StandardScaler
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
scaled_df = scaler.fit(df).transform(df)``` |
| `MinMaxScaler` | Scales features to range | ```from pyspark.ml.feature import MinMaxScaler
scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures", min=0.0, max=1.0)
scaled_df = scaler.fit(df).transform(df)``` |
| `PCA` | Dimension reduction | ```from pyspark.ml.feature import PCA
pca = PCA(k=3, inputCol="features", outputCol="pca
