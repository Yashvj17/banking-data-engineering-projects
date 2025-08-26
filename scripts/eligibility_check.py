from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("EligibilityCheck").getOrCreate()

df = spark.read.parquet("../data/processed/cleaned_applications")

# Eligibility rules
eligible_df = df.filter((col("age") >= 21) & 
                        (col("annual_income") >= 300000) & 
                        (col("credit_score") >= 700))

rejected_df = df.exceptAll(eligible_df)

# Save outputs
eligible_df.write.mode("overwrite").parquet("../data/processed/eligible_customers")
rejected_df.write.mode("overwrite").parquet("../data/rejected/rejected_customers")

print("✅ Eligibility check complete.")
print(f"Approved: {eligible_df.count()} | Rejected: {rejected_df.count()}")
