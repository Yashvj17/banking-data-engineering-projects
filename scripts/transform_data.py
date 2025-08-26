from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, lower, trim, to_date, datediff, current_date

spark = SparkSession.builder.appName("CreditCardOnboarding").getOrCreate()

# Read raw data
df = spark.read.csv("../data/raw/applications.csv", header=True, inferSchema=True)

# Clean & transform data
df_clean = (
    df.dropDuplicates(["email", "phone"])
      .withColumn("full_name", trim(col("full_name")))
      .withColumn("email", lower(col("email")))
      .withColumn("phone", trim(col("phone")))
      .withColumn("dob", to_date(col("dob"), "yyyy-MM-dd"))
      .withColumn("age", (datediff(current_date(), col("dob")) / 365).cast("int"))
      .withColumn("city", upper(col("city")))
      .withColumn("state", upper(col("state")))
)

# Save transformed data
df_clean.write.mode("overwrite").parquet("../data/processed/cleaned_applications")

print("✅ Data transformed and stored in processed folder.")
