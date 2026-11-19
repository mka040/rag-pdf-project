# Databricks notebook source
# MAGIC %pip install openai -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from openai import OpenAI
import time
# ---- Auto-fetch token (secure, no hardcoding) ----
DATABRICKS_TOKEN = dbutils.notebook.entry_point \
    .getDbutils().notebook().getContext().apiToken().get()
DATABRICKS_HOST = spark.conf.get("spark.databricks.workspaceUrl")
client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{DATABRICKS_HOST}/serving-endpoints"
)
print(f"Connected to: {DATABRICKS_HOST}")
print(f"Token: {DATABRICKS_TOKEN[:8]}... (auto-fetched)")
# Quick test — verify embedding model works
test_response = client.embeddings.create(
    model="databricks-bge-large-en",
    input=["test embedding"]
)
dim = len(test_response.data[0].embedding)
print(f"Embedding model ready! Dimension: {dim}")

# COMMAND ----------

# DBTITLE 1,Generate embeddings for all chunks
# Load chunks from Delta
df_chunks = spark.table("dev.rag_project.chunks")
chunk_data = df_chunks.select("chunk_id", "text").collect()
texts = [row.text for row in chunk_data]
chunk_ids = [row.chunk_id for row in chunk_data]
print(f"Generating embeddings for {len(texts)} chunks...")
print(f"Model: databricks-bge-large-en (FREE)")
# Batch embedding
BATCH_SIZE = 10 # Smaller batches for Foundation Model rate limits
all_embeddings = []
for i in range(0, len(texts), BATCH_SIZE):
    batch = texts[i:i + BATCH_SIZE]
    response = client.embeddings.create(
        model="databricks-bge-large-en",
        input=batch
    )
    batch_embs = [item.embedding for item in response.data]
    all_embeddings.extend(batch_embs)
    print(f" Batch {i//BATCH_SIZE + 1}/{-(-len(texts)//BATCH_SIZE)}: "
        f"{len(batch)} chunks embedded")
    time.sleep(1) # Rate limit safety for free tier
print(f"\nDone! {len(all_embeddings)} embeddings generated")
print(f"Embedding dimension: {len(all_embeddings[0])}")
print(f"Cost: $0.00 (Databricks Foundation Models are free!)")


# COMMAND ----------

# DBTITLE 1,Save to Delta
from pyspark.sql import Row
from pyspark.sql.types import (StructType, StructField, IntegerType,
StringType, ArrayType, FloatType)
schema = StructType([
        StructField("chunk_id", IntegerType(), False),
        StructField("text", StringType(), False),
        StructField("embedding", ArrayType(FloatType()), False)
])
emb_rows = []
for cid, text, emb in zip(chunk_ids, texts, all_embeddings):
    emb_rows.append((int(cid), text, [float(x) for x in emb]))
df_embeddings = spark.createDataFrame(emb_rows, schema=schema)
df_embeddings.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dev.rag_project.embeddings")
print(f"Saved {df_embeddings.count()} embeddings to dev.rag_project.embeddings")
display(df_embeddings.select("chunk_id", "text"))