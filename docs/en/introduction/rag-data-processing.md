# RAG-Ready Data Processing

> How to use SeaTunnel to build Retrieval-Augmented Generation (RAG) data pipelines: parse documents, split text into chunks, generate embeddings, and load vector databases — in batch or streaming.

## Why a data pipeline matters for RAG

Retrieval-Augmented Generation (RAG) is only as good as the data behind it. The path from raw knowledge — files, wikis, databases — to a vector database involves several stages: parsing documents into text, splitting text into chunks, optionally enriching or cleaning it with an LLM, converting chunks into embeddings, and loading them into a vector store that stays fresh over time.

SeaTunnel covers all of these stages with its existing plugin model, so the same engine that moves your business data can also build and maintain your knowledge base: the same configuration style, the same parallelism and fault-tolerance behavior, and the same batch/streaming execution modes.

## Building blocks

| Stage | SeaTunnel plugin | Documentation |
| --- | --- | --- |
| Document parsing | LocalFile source with `file_format_type = markdown` or `pdf` | [LocalFile](../connectors/source/LocalFile.md) |
| Database ingestion | JDBC sources such as MySQL | [MySQL](../connectors/source/Mysql.md) |
| Change data capture | MySQL-CDC source | [MySQL-CDC](../connectors/source/MySQL-CDC.md) |
| Text splitting | TextChunk transform | [TextChunk](../transforms/text-chunk.md) |
| Simple field splitting | Split transform | [Split](../transforms/split.md) |
| Metadata projection | Metadata transform | [Metadata](../transforms/metadata.md) |
| LLM processing | LLM transform | [LLM](../transforms/llm.md) |
| Vectorization | Embedding transform | [Embedding](../transforms/embedding.md) |
| Vector storage | Milvus / Qdrant sinks | [Milvus](../connectors/sink/Milvus.md), [Qdrant](../connectors/sink/Qdrant.md) |

## Pipeline at a glance

```
 documents / tables
        │
        ▼
 parse (LocalFile: markdown / pdf)  ── or ──  JDBC / CDC source
        │                                          │
        ▼                                          ▼
 chunk (TextChunk) ────────► enrich (LLM, Metadata) ───► embed (Embedding)
                                                            │
                                                            ▼
                                              vector store (Milvus / Qdrant)
```

## Example 1: build a document knowledge base in Milvus

A batch job that parses a directory of Markdown documents, re-chunks the text with overlap, embeds each chunk, and writes everything to Milvus:

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  LocalFile {
    path = "/data/knowledge-base"
    file_format_type = "markdown"
    markdown_rag_metadata_enabled = true
  }
}

transform {
  TextChunk {
    text_field = "text"
    output_field = "chunk"
    chunk_index_field = "chunk_seq"
    chunk_size = 800
    overlap_size = 100
  }
  Embedding {
    model_provider = OPENAI
    api_key = "sk-xxxx"
    model = "text-embedding-3-small"
    dimension = 1024
    single_vectorized_input_number = 10
    model_retry_max_attempts = 5
    vectorization_fields {
      chunk = "chunk_vector"
    }
  }
}

sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "root:Milvus"
    collection = "knowledge_base"
    enable_upsert = false
    batch_size = 500
  }
}
```

What happens at each stage:

1. **Parse.** The LocalFile source parses each Markdown file into element rows (headings, paragraphs, list items, code blocks, ...). Each row carries fields such as `text`, `element_id`, `element_type`, `heading_level` and `position_index`. With `markdown_rag_metadata_enabled = true` the source additionally appends RAG metadata columns: `source_uri`, `document_id`, `chunk_id`, `chunk_index` and `content_hash` — stable identifiers derived from the file path and content.
2. **Chunk.** The TextChunk transform splits each element's `text` into chunks of at most 800 characters, keeping about 100 characters of overlap between adjacent chunks. Every output row keeps all original fields and adds the chunk text (`chunk`) and its position within the source row (`chunk_seq`).
3. **Embed.** The Embedding transform calls the embedding model and writes the vector into a new field `chunk_vector` with dimension 1024.
4. **Load.** The Milvus sink creates the target collection from the upstream schema if it does not exist and writes in batches of 500 records.

> Note: `chunk_index` produced by the Markdown parser is 1-based, while the index emitted by TextChunk (`chunk_index_field`) is 0-based. This example renames TextChunk's field to `chunk_seq` so the two never collide.

To ingest PDF files instead, use `file_format_type = "pdf"` together with `pdf_rag_metadata_enabled = true`.

## Example 2: vectorize database rows into Qdrant

The same pipeline pattern works for structured data — here, product descriptions are embedded and written to Qdrant:

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  MySQL {
    url = "jdbc:mysql://127.0.0.1:3306/shop"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "******"
    table_path = "shop.products"
  }
}

transform {
  Embedding {
    model_provider = OPENAI
    api_key = "sk-xxxx"
    model = "text-embedding-3-small"
    dimension = 1024
    vectorization_fields {
      description = "description_vector"
    }
  }
}

sink {
  Qdrant {
    collection_name = "product_vectors"
    host = "127.0.0.1"
    port = 6334
  }
}
```

## Keeping vectors fresh

Knowledge bases are living systems: documents are edited and database rows change. SeaTunnel supports several refresh strategies:

- **Full rebuild.** Re-run the job with `data_save_mode = DROP_DATA` on the Milvus sink to drop and rebuild the collection from scratch. Simple and always consistent, at the cost of a full re-embedding run.
- **Idempotent updates.** If the upstream schema carries a primary key, set `enable_upsert = true` on the Milvus sink so re-running the job updates existing records instead of duplicating them.
- **Change data capture.** Use the MySQL-CDC source to stream row changes and apply them to the vector store continuously. Pair CDC with an upsert-capable sink so updated rows are re-embedded and overwritten in place.
- **Chunk-level lifecycle.** Be aware that SeaTunnel does not automatically delete stale chunks when a source document changes (no tombstones). If you re-chunk documents and the number of chunks shrinks, old chunks remain. Either rebuild periodically, or design stable chunk identifiers and handle deletions in a follow-up step of your own pipeline.

## Best practices

- **Chunk size.** `chunk_size` is counted in Unicode code points and defaults to 1000. Keep `overlap_size` well below `chunk_size` — the chunk count grows roughly as `chunk_size / (chunk_size - overlap_size)`. Use `separators` to avoid cutting mid-sentence.
- **Embedding reliability.** Remote embedding APIs are rate-limited and occasionally fail. Set `model_retry_max_attempts` greater than 1 so retryable failures (rate limiting, timeouts) are retried with backoff instead of failing the job. Use `single_vectorized_input_number` to batch multiple inputs into one request when your provider allows it.
- **Precision.** Embedding vectors are produced in `float32` only; other precisions are converted automatically. Make sure the vector dimension configured on the Embedding transform matches the vector field expected by the target collection.
- **Primary keys.** TextChunk requires that `output_field` / `chunk_index_field` do not collide with columns participating in a primary or unique key. When upserting into Milvus, remember that upsert requires a primary key in the collection schema.
- **Metadata filtering.** With `markdown_rag_metadata_enabled = true`, each row carries `document_id`, `chunk_id` and `content_hash`. Use the Metadata transform to project the logical knowledge-sync fields (for example `SourceUri`, `DocumentId`, `ChunkHash`) into application-specific columns for filtering at retrieval time.

## FAQ

### Can SeaTunnel delete stale chunks when a document changes?

Not automatically. SeaTunnel moves and transforms data; chunk deletion policies belong to the consuming pipeline. See [Keeping vectors fresh](#keeping-vectors-fresh) for the supported strategies.

### Which embedding model providers are supported?

Common providers such as `OPENAI`, `AMAZON`, `DOUBAO` and `QIANFAN` are supported, plus a `CUSTOM` provider where you define request headers, body and response parsing yourself. See the [Embedding](../transforms/embedding.md) documentation for the full list and options.

### Do I have to re-embed everything when one document changes?

No — with `markdown_rag_metadata_enabled = true` each chunk carries a `content_hash` and stable identifiers, so downstream systems can detect which chunks changed. Note that if a transform between source and sink changes the text or expands rows (like TextChunk), the final chunk identifiers and hashes should be recomputed after that transform.
