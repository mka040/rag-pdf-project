# Databricks notebook source
import re
from pyspark.sql.functions import udf, col, length, lit, current_timestamp
from pyspark.sql.types import StringType
# Load raw pages from Delta
df_raw = spark.table("dev.rag_project.raw_pages")
# Cleaning UDF
@udf(returnType=StringType())
def clean_text(text):
    if not text:
        return ""
    # 1. Normalize excessive newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    # 2. Collapse multiple spaces
    text = re.sub(r' {2,}', ' ', text)
    # 3. Remove page number artifacts
    text = re.sub(r'Page \d+ of \d+', '', text)
    # 4. Fix soft hyphens from PDF extraction
    text = text.replace('\xad', '-')
    # 5. Join lines broken mid-sentence (lowercase after newline)
    text = re.sub(r'\n([a-z])', r' \1', text)
    # 6. Strip whitespace
    text = text.strip()
    return text
# Apply cleaning + add metadata columns
df_clean = (df_raw
    .withColumn("clean_text", clean_text(col("text")))
    .withColumn("clean_char_count", length(col("clean_text")))
    .withColumn("source_file", lit("sample_company_policy.pdf"))
    .withColumn("processed_at", current_timestamp())
)
# Save cleaned data
df_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dev.rag_project.clean_pages")
# Show results
display(df_clean.select(
    "page_number", "char_count", "clean_char_count", "source_file"
))
print("Cleaning complete! Check char_count vs clean_char_count for noise removed.")

# COMMAND ----------

