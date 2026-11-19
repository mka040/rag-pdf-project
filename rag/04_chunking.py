# Databricks notebook source
# DBTITLE 1,Install LangChain
# MAGIC %pip install langchain-text-splitters -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configure and run chunking
from langchain_text_splitters import RecursiveCharacterTextSplitter
# Load cleaned text
df_clean = spark.table("dev.rag_project.clean_pages")
pages = df_clean.orderBy("page_number").collect()
# Combine all pages into one document
full_text = "\n\n".join([row.clean_text for row in pages])
print(f"Total document length: {len(full_text):,} characters")
print(f"Estimated tokens: ~{len(full_text)//4:,}")
# ===========================================
# KEY PARAMETERS — experiment with these!
# ===========================================
CHUNK_SIZE = 500 # chars per chunk (~125 tokens)
CHUNK_OVERLAP = 100 # overlap between chunks (20%)
splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
    separators=["\n\n", "\n", ". ", ", ", " ", ""],
    length_function=len,
)
chunks = splitter.split_text(full_text)
print(f"\nChunking config: size={CHUNK_SIZE}, overlap={CHUNK_OVERLAP}")
print(f"Created {len(chunks)} chunks")
print(f"Avg chunk size: {sum(len(c) for c in chunks) // len(chunks)} chars")
# Preview first 3 chunks
for i, chunk in enumerate(chunks[:3]):
    print(f"\n{'='*50}")
    print(f"CHUNK {i+1} ({len(chunk)} chars):")
    print(f"{'='*50}")
    print(chunk[:300])
    if len(chunk) > 300:
        print("...")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit
# Create chunk records with rich metadata
chunk_records = []

# COMMAND ----------

for i, chunk in enumerate(chunks):
    chunk_records.append(Row(
        chunk_id=i,
        text=chunk,
        char_count=len(chunk),
        token_estimate=len(chunk) // 4,
        source_file="sample_company_policy.pdf",
        doc_type="employee_handbook",
        chunk_strategy=f"recursive_{CHUNK_SIZE}_{CHUNK_OVERLAP}"
))
df_chunks = spark.createDataFrame(chunk_records) \
    .withColumn("created_at", current_timestamp())
df_chunks.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dev.rag_project.chunks")
print(f"Saved {df_chunks.count()} chunks to dev.rag_project.chunks")
display(df_chunks)

# COMMAND ----------

