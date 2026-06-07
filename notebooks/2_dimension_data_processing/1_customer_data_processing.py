# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/midhungretta@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customer", "Data Source")
             

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
base_path = f's3://sportsbar-data-s3/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = (
    spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)

display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Processing

# COMMAND ----------

df_bronze = spark.sql(
    f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};"
)

df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

df_duplicates = df_bronze.groupBy("customer_id").count().filter(F.col("count") > 1)

display(df_duplicates)

# COMMAND ----------

print('Rows before duplicates dropped: ', df_bronze.count())

df_silver = df_bronze.dropDuplicates(['customer_id'])

print('Rows after duplicates dropped: ', df_silver.count())

# COMMAND ----------

# check those values

display(
    df_silver.filter(
        F.col("customer_name") != F.trim(F.col("customer_name"))
    )
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name",
    F.trim(F.col("customer_name"))
)

# COMMAND ----------

# check those values

display(
    df_silver.filter(
        F.col("customer_name") != F.trim(F.col("customer_name"))
    )
)

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

# typos + correct names

city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',

    'Hyderabadd': 'Hyderabad',
    'Hydrabad': 'Hyderabad',

    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDehlee': 'New Delhi'
}
allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(), None)
         .when(F.col("city").isin(allowed), F.col("city"))
         .otherwise(None)
    )
)
df_silver.select('city').distinct().show()

# COMMAND ----------

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

# Title case fix

df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(), None)
     .otherwise(F.initcap("customer_name"))
)

df_silver.select("customer_name").distinct().show()

# COMMAND ----------

df_silver.filter(F.col('city').isNull()).show(truncate=False)

# COMMAND ----------

null_customer_names = [
    "Sprintx Nutrition",
    "Zenathlete Foods",
    "Primefuel Nutrition",
    "Recovery Lane"
]

df_silver.filter(
    F.col("customer_name").isin(null_customer_names)
).show(truncate=False)

# COMMAND ----------

# Business Confirmation Note: City corrections confirmed by business team

customer_city_fix = {

    # Sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",

    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"
}
df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")  # Replace null with fixed city
    )
    .drop("fixed_city")
)
display(df_silver)

# COMMAND ----------

# cast customer_id to string, because parent company id is in string

df_silver = df_silver.withColumn( "customer_id", F.col("customer_id").cast("string") )

print(df_silver.printSchema())

# COMMAND ----------

df_silver = (
    df_silver

    # Build final customer column: "CustomerName-City" or "CustomerName-Unknown"
    .withColumn(
        "customer",
        F.concat_ws(
            "-",
            "customer_name",
            F.coalesce(F.col("city"), F.lit("Unknown"))
        )
    )

    # Static attributes aligned with parent data model
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)
display(df_silver.limit(5))

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Gold Processing
# MAGIC  

# COMMAND ----------

df_silver = spark.sql(
    f"SELECT * FROM {catalog}.{silver_schema}.{data_source};"
)

# take required cols only
# customer_id, customer_name, city, read_timestamp,
# file_name, file_size, customer, market, platform, channel

df_gold = df_silver.select(
    "customer_id",
    "customer_name",
    "city",
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

df_gold.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim{data_source}")

# COMMAND ----------

delta_table = DeltaTable.forName(
    spark,
    "fmcg.gold.dim_customers"
)

df_child_customers = spark.table(
    "fmcg.gold.dim_customers"
).select(
    "customer_code",
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

## Update record if exist or insert it if not exist
## add child data with our parent company

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

