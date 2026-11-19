# Data Engineering Implementations on Databricks

Hands-on implementations built on Databricks: an end-to-end RAG pipeline over PDFs,
and a medallion-architecture streaming pipeline with Spark Declarative Pipelines
(formerly DLT).

## 1. RAG over PDFs (`*.py` in root)

An end-to-end Retrieval-Augmented Generation (RAG) pipeline built entirely in Databricks:
ingest PDFs, extract and clean text, chunk it, generate embeddings, index them for
similarity search, and answer questions with a RAG agent.

| Step | Notebook | What it does |
| ---- | -------- | ------------ |
| 1 | `01_upload_pdf` | Upload source PDF documents |
| 2 | `02_extract_text` | Extract raw text from the PDFs |
| 3 | `03_clean_text` | Clean and normalize the extracted text |
| 4 | `04_chunking` | Split text into retrieval-sized chunks |
| 5 | `05_embeddings` | Generate vector embeddings for each chunk |
| 6 | `06_vector_index` | Build a vector index for similarity search |
| 7 | `07_rag_agent` | RAG agent: retrieve relevant chunks and answer questions |

Run the notebooks in numbered order (1 -> 7). They use Databricks-native services
(Unity Catalog, Databricks vector search) for storage, indexing, and retrieval.

## 2. DLT Medallion Pipeline (`dlt/`)

A bronze -> silver -> gold streaming pipeline using Spark Declarative Pipelines
(Auto Loader + streaming tables), built on TPCH order/customer data.

| Notebook | What it does |
| -------- | ------------ |
| `dlt/setup_tables` | One-time setup: `bronze` schema (deep-cloned from `samples.tpch`), `etl.landing` catalog/schema, and UC volumes for source files and Auto Loader schemas |
| `dlt/dlt_introduction_autoloader` | End-to-end pipeline: bronze streaming tables (from `bronze.order_raw` + Auto Loader CSV ingestion from a UC volume, unioned), a silver orders-joined-to-customers table with an audit column, and a gold aggregation of order counts per market segment |

Run `dlt/setup_tables` first, then attach `dlt/dlt_introduction_autoloader`
to a pipeline as a source notebook and start an update.

## Author

Manish Acharya — built as hands-on implementations on Databricks.
