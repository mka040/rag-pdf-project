# Databricks notebook source
# MAGIC %pip install pdfplumber -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Extract text page by page
import pdfplumber
# Read from Unity Catalog Volume
pdf_path = "/Volumes/dev/rag_project/files/sample_company_policy.pdf"
page_texts = []
with pdfplumber.open(pdf_path) as pdf:
    print(f"Total pages: {len(pdf.pages)}")
    for i, page in enumerate(pdf.pages):
        text = page.extract_text()
        if text:
            page_texts.append({
                "page_number": i + 1,
                "text": text,
                "char_count": len(text)
            })
            print(f" Page {i+1}: {len(text)} characters extracted")
# Also extract tables (important for structured data)
with pdfplumber.open(pdf_path) as pdf:
    for i, page in enumerate(pdf.pages):
            tables = page.extract_tables()
            if tables:
                    print(f" Page {i+1}: Found {len(tables)} table(s)")
print(f"\nTotal pages with text: {len(page_texts)}")

# COMMAND ----------

# DBTITLE 1,Save to Delta Table
from pyspark.sql import Row
# Convert to Spark DataFrame
rows = [Row(**p) for p in page_texts]
df_pages = spark.createDataFrame(rows)
# Save as Delta table in our schema
df_pages.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dev.rag_project.raw_pages")
print("Saved to dev.rag_project.raw_pages")
display(df_pages)