# Databricks notebook source
# MAGIC %pip install faiss-cpu numpy openai -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Build FAISS index and test retrieval:
import faiss
import numpy as np
from openai import OpenAI
# ---- Setup client ----
DATABRICKS_TOKEN = dbutils.notebook.entry_point \
    .getDbutils().notebook().getContext().apiToken().get()
DATABRICKS_HOST = spark.conf.get("spark.databricks.workspaceUrl")
client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{DATABRICKS_HOST}/serving-endpoints"
)
# ---- Load embeddings from Delta ----
df_emb = spark.table("dev.rag_project.embeddings")
rows = df_emb.collect()
texts = [row.text for row in rows]
embeddings = np.array([row.embedding for row in rows]).astype('float32')
print(f"Loaded {len(texts)} chunks, {embeddings.shape[1]}-dim embeddings")
# ---- Build FAISS index ----
dimension = embeddings.shape[1]
index = faiss.IndexFlatL2(dimension) # L2 = Euclidean distance
index.add(embeddings)
print(f"FAISS index built: {index.ntotal} vectors")
# ---- Search function ----
def search_similar(query, top_k=3):
    q_emb = client.embeddings.create(
        model="databricks-bge-large-en",
        input=[query]
    ).data[0].embedding
    q_vec = np.array([q_emb]).astype('float32')
    distances, indices = index.search(q_vec, top_k)
    return [(texts[idx], float(dist)) for idx, dist in zip(indices[0], distances[0])]
# ---- Test searches ----
test_queries = [
    "remote work policy",
    "parental leave",
    "performance rating promotion",
    "expense travel limit",
]
for q in test_queries:
    print(f"\n{'='*55}")
    print(f"QUERY: {q}")
    results = search_similar(q, top_k=2)
    for i, (text, dist) in enumerate(results):
        print(f" Rank {i+1} (dist: {dist:.4f}): {text[:120]}...")

# COMMAND ----------

