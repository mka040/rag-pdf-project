# RAG over PDFs on Databricks

An end-to-end Retrieval-Augmented Generation (RAG) pipeline built entirely in Databricks:
ingest PDFs, extract and clean text, chunk it, generate embeddings, index them for
similarity search, and answer questions with a RAG agent.

## Pipeline

| Step | Notebook | What it does |
| ---- | -------- | ------------ |
| 1 | `01_upload_pdf` | Upload source PDF documents |
| 2 | `02_extract_text` | Extract raw text from the PDFs |
| 3 | `03_clean_text` | Clean and normalize the extracted text |
| 4 | `04_chunking` | Split text into retrieval-sized chunks |
| 5 | `05_embeddings` | Generate vector embeddings for each chunk |
| 6 | `06_vector_index` | Build a vector index for similarity search |
| 7 | `07_rag_agent` | RAG agent: retrieve relevant chunks and answer questions |

## Running

Open the notebooks in a Databricks workspace and run them in numbered order
(1 -> 7). The notebooks use Databricks-native services (Unity Catalog,
Databricks vector search) for storage, indexing, and retrieval.
