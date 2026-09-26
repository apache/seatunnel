# RAG Ready 数据处理

> 介绍如何使用 SeaTunnel 构建 RAG(Retrieval-Augmented Generation,检索增强生成)数据管道:解析文档、切分文本、生成向量,并写入向量数据库 —— 支持批式与流式。

## 为什么 RAG 需要一条数据管道

RAG 的效果取决于它背后的数据。从原始知识(文件、Wiki、数据库)到向量数据库,通常要经过多个阶段:把文档解析成文本、把文本切分成块、(可选)用 LLM 做清洗或增强、把文本块转换为向量,再把向量写入一个能持续保持新鲜的向量库。

SeaTunnel 用它现有的插件体系覆盖了上述全部阶段 —— 那套搬运业务数据的引擎,同样可以用来构建和维护你的知识库:一样的配置风格、一样的并行与容错语义、一样的批式/流式执行模式。

## 构建模块

| 阶段 | SeaTunnel 插件 | 文档 |
| --- | --- | --- |
| 文档解析 | LocalFile source,`file_format_type = markdown` 或 `pdf` | [LocalFile](../connectors/source/LocalFile.md) |
| 数据库接入 | MySQL 等 JDBC source | [MySQL](../connectors/source/Mysql.md) |
| 变更数据捕获 | MySQL-CDC source | [MySQL-CDC](../connectors/source/MySQL-CDC.md) |
| 文本切分 | TextChunk transform | [TextChunk](../transforms/text-chunk.md) |
| 简单字段拆分 | Split transform | [Split](../transforms/split.md) |
| 元数据投影 | Metadata transform | [Metadata](../transforms/metadata.md) |
| LLM 处理 | LLM transform | [LLM](../transforms/llm.md) |
| 向量化 | Embedding transform | [Embedding](../transforms/embedding.md) |
| 向量存储 | Milvus / Qdrant sink | [Milvus](../connectors/sink/Milvus.md),[Qdrant](../connectors/sink/Qdrant.md) |

## 管道一览

```
 文档 / 数据表
      │
      ▼
 解析(LocalFile:markdown / pdf)── 或 ── JDBC / CDC source
      │                                          │
      ▼                                          ▼
 切分(TextChunk)────► 增强(LLM、Metadata)──► 向量化(Embedding)
                                                       │
                                                       ▼
                                          向量库(Milvus / Qdrant)
```

## 示例一:在 Milvus 中构建文档知识库

一个批式作业:解析 Markdown 文档目录 → 带重叠地重新切分文本 → 对每个文本块做向量化 → 全部写入 Milvus:

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

每个阶段发生的事情:

1. **解析。** LocalFile source 把每个 Markdown 文件解析成元素行(标题、段落、列表项、代码块等),每行携带 `text`、`element_id`、`element_type`、`heading_level`、`position_index` 等字段。开启 `markdown_rag_metadata_enabled = true` 后,source 还会附加 RAG 元数据列:`source_uri`、`document_id`、`chunk_id`、`chunk_index`、`content_hash` —— 由文件路径与内容推导出的稳定标识。
2. **切分。** TextChunk transform 把每个元素的 `text` 切成不超过 800 字符的文本块,相邻块之间保留约 100 字符的重叠。每个输出行保留全部原始字段,并追加块文本(`chunk`)及其在源行中的序号(`chunk_seq`)。
3. **向量化。** Embedding transform 调用向量模型,把 `chunk` 转换为 1024 维向量,写入新字段 `chunk_vector`。
4. **写入。** Milvus sink 在目标集合不存在时按上游 schema 自动建集合,并按每批 500 条写入。

> 注意:Markdown 解析器产出的 `chunk_index` 是从 1 开始的,而 TextChunk 自带的序号字段(`chunk_index_field`)是从 0 开始的。示例里把 TextChunk 的字段改名为 `chunk_seq`,避免两者冲突。

如果要摄入 PDF 文件,把 `file_format_type` 设为 `"pdf"` 并开启 `pdf_rag_metadata_enabled = true` 即可。

## 示例二:把数据库行向量化后写入 Qdrant

同一套管道模式也适用于结构化数据 —— 这里把商品描述向量化后写入 Qdrant:

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

## 让向量保持新鲜

知识库是活的:文档会被编辑,数据库的行会变化。SeaTunnel 支持几种刷新策略:

- **全量重建。** 把 Milvus sink 的 `data_save_mode` 设为 `DROP_DATA` 后重跑作业,丢弃旧集合并全量重建。最简单、永远一致,代价是重新跑一遍向量化。
- **幂等更新。** 如果上游 schema 带有主键,把 Milvus sink 的 `enable_upsert` 设为 `true`,重跑作业时将按主键更新已有记录,而不是重复写入。
- **变更数据捕获。** 使用 MySQL-CDC source 持续把行变更应用到向量库。CDC 需要搭配支持 upsert 的 sink,这样被更新的行会重新向量化并原地覆盖。
- **块级生命周期。** 注意:当源文档发生变化时,SeaTunnel 不会自动删除过期的文本块(没有墓碑机制)。如果重新切分后块的数量变少了,旧块会残留。要么定期全量重建,要么自行设计稳定的块标识,并在管道的后续步骤里处理删除。

## 最佳实践

- **块大小。** `chunk_size` 按 Unicode 码点计数,默认 1000。`overlap_size` 要远小于 `chunk_size` —— 块数量大约按 `chunk_size / (chunk_size - overlap_size)` 增长。善用 `separators` 避免把句子从中间切断。
- **向量化的可靠性。** 远程向量化 API 有限流,偶发失败。把 `model_retry_max_attempts` 设为大于 1,让限流、超时这类可重试错误按退避策略自动重试,而不是让作业直接失败。在服务商允许时,用 `single_vectorized_input_number` 把多条输入合并进一次请求。
- **精度。** 向量目前只产出 `float32`,其他精度会被自动转换。务必保证 Embedding transform 配置的向量维度与目标集合期望的向量字段维度一致。
- **主键。** TextChunk 要求 `output_field` / `chunk_index_field` 不能与主键或唯一键涉及的列重名。向 Milvus 写入 upsert 时,记住 upsert 要求集合 schema 中有主键。
- **元数据过滤。** 开启 `markdown_rag_metadata_enabled = true` 后,每行都带有 `document_id`、`chunk_id` 和 `content_hash`。可以用 Metadata transform 把逻辑知识同步字段(如 `SourceUri`、`DocumentId`、`ChunkHash`)投影成应用自己的列,便于检索时过滤。

## FAQ

### 文档变化时,SeaTunnel 会删除过期的文本块吗?

不会自动删除。SeaTunnel 负责数据的搬运与转换,块删除策略属于消费方管道的职责。支持的策略见[让向量保持新鲜](#让向量保持新鲜)。

### 支持哪些向量模型服务商?

支持 `OPENAI`、`AMAZON`、`DOUBAO`、`QIANFAN` 等常见服务商,另外提供 `CUSTOM` 模式 —— 自行定义请求头、请求体和响应解析。完整列表与选项见 [Embedding](../transforms/embedding.md) 文档。

### 一个文档变化后,必须把所有内容重新向量化一遍吗?

不需要 —— 开启 `markdown_rag_metadata_enabled = true` 后,每个文本块都带有 `content_hash` 和稳定标识,下游系统可以据此判断哪些块发生了变化。注意:如果 source 与 sink 之间的 transform 改变了文本或将一行展开成多行(例如 TextChunk),最终的块标识和哈希应当在该 transform 之后重新计算。
