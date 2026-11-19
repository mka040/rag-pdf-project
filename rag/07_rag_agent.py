# Databricks notebook source
# MAGIC %pip install openai faiss-cpu numpy -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import faiss, numpy as np
from openai import OpenAI
# ---- Secure client setup ----
DATABRICKS_TOKEN = dbutils.notebook.entry_point \
    .getDbutils().notebook().getContext().apiToken().get()
DATABRICKS_HOST = spark.conf.get("spark.databricks.workspaceUrl")
client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{DATABRICKS_HOST}/serving-endpoints"
)
# ---- Rebuild index from Delta ----
df_emb = spark.table("dev.rag_project.embeddings")
rows = df_emb.collect()
texts = [row.text for row in rows]
embeddings = np.array([row.embedding for row in rows]).astype('float32')
index = faiss.IndexFlatL2(embeddings.shape[1])
index.add(embeddings)
print(f"Agent ready! Index: {index.ntotal} vectors | "
    f"LLM: databricks-meta-llama-3-1-8b-instruct")

# COMMAND ----------

# DBTITLE 1,Define the complete RAG Agent
def retrieve(query, top_k=3):
    """Step 1: Vector similarity search."""
    q_emb = client.embeddings.create(
        model="databricks-bge-large-en", input=[query]
    ).data[0].embedding
    q_vec = np.array([q_emb]).astype('float32')
    distances, indices = index.search(q_vec, top_k)
    return [(texts[idx], float(dist))
            for idx, dist in zip(indices[0], distances[0])]

def rag_agent(question, top_k=3, temperature=0.1):
    """Complete RAG: Retrieve → Augment → Generate."""
    # ---- RETRIEVE ----
    retrieved = retrieve(question, top_k=top_k)
    context = "\n\n---\n\n".join([t for t, d in retrieved])
    
    # ---- AUGMENT (build grounded prompt) ----
    system_prompt = """You are a helpful HR assistant for TechVision Inc.
Answer questions ONLY based on the provided context from the
Employee Handbook.

Rules:
1. ONLY use information from the context below
2. If the answer is not in the context, say:
   "I don't have that information in the Employee Handbook."
3. Be specific - cite numbers, dates, exact policies
4. Do NOT make up or infer information not in the context
5. Keep answers concise but complete"""
    
    user_prompt = f"""CONTEXT FROM EMPLOYEE HANDBOOK:
{context}

EMPLOYEE QUESTION: {question}

Answer based strictly on the context above:"""
    
    # ---- GENERATE ----
    response = client.chat.completions.create(
        model="databricks-meta-llama-3-1-8b-instruct",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=temperature,
        max_tokens=500
    )
    
    answer = response.choices[0].message.content
    
    return {
        "question": question,
        "answer": answer,
        "sources": [t[:100] + "..." for t, d in retrieved],
        "distances": [d for t, d in retrieved],
        "model": "databricks-meta-llama-3-1-8b-instruct",
        "embedding_model": "databricks-bge-large-en",
        "chunks_retrieved": len(retrieved)
    }

print("RAG Agent defined! Run next cell to test.")

# COMMAND ----------

test_questions = [
"How many remote work days per week are allowed?",
"What is the annual leave entitlement?",
"What rating do I need for a promotion?",
"What is the expense limit for international travel?",
"Is multi-factor authentication required?",
"How long is parental leave?",
"What happens if I get a performance rating of 2?",
"What is the home office setup budget?",
"Can I work remotely from another country?",
]
print("=" * 65)
print(" RAG AGENT TEST SUITE")
print(f" LLM: databricks-meta-llama-3-1-8b-instruct (FREE)")
print(f" Embeddings: databricks-bge-large-en (FREE)")
print("=" * 65)
for q in test_questions:
    result = rag_agent(q)
    print(f"\nQ: {result['question']}")
    print(f"A: {result['answer']}")
    print(f" [Chunks: {result['chunks_retrieved']}, "
    f"Top distance: {result['distances'][0]:.4f}]")
    print("-" * 65)

# COMMAND ----------

print("TechVision HR Assistant (powered by Llama 3.1)")
print("Type 'quit' to exit\n")
while True:
    question = input("You: ")
    if question.lower().strip() in ['quit', 'exit', 'q', '']:
            print("Goodbye!")
            break
    result = rag_agent(question)
    print(f"\nAssistant: {result['answer']}")
    print(f" ({result['chunks_retrieved']} chunks, "
    f"dist: {result['distances'][0]:.4f})\n")

# COMMAND ----------

