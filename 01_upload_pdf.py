# Databricks notebook source
# DBTITLE 1,Create schema and volume
# MAGIC %sql
# MAGIC -- Create a dedicated schema for our RAG project
# MAGIC CREATE SCHEMA IF NOT EXISTS dev.rag_project;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Volume to store files (like Unity Catalog Volumes in paid tier)
# MAGIC CREATE VOLUME IF NOT EXISTS dev.rag_project.files;

# COMMAND ----------

# Verify the PDF is accessible
import os
volume_path = "/Volumes/dev/rag_project/files"
files = os.listdir(volume_path)
print("Files in volume:")
for f in files:
    size = os.path.getsize(os.path.join(volume_path, f))
    print(f" {f} — {size:,} bytes")

# COMMAND ----------

# DBTITLE 1,Cell 4
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

# ---- Verify PDF uploaded ----
volume_path = "/Volumes/dev/rag_project/files"
files = os.listdir(volume_path)
print("Files in volume:")
for f in files:
    size = os.path.getsize(os.path.join(volume_path, f))
    print(f" {f} - {size:,} bytes")

# ---- Test Foundation Model API ----
w = WorkspaceClient()

# Quick test - does the LLM respond?
response = w.serving_endpoints.query(
    name="databricks-meta-llama-3-1-8b-instruct",
    messages=[
        ChatMessage(role=ChatMessageRole.USER, content="Say hello in one sentence.")
    ],
    max_tokens=50
)

print(f"\nLLM test: {response.choices[0].message.content}")
print("\nAll systems ready! Proceed to Notebook 02.")