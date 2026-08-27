import ChangeLog from '../changelog/connector-google-firestore.md';

# GoogleFirestore

> Google Firestore Sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

GoogleFirestore Sink 用于将 SeaTunnel 数据写入 Google Cloud Firestore 集合。

每一行 SeaTunnel 数据会转换为一个 Firestore 文档。连接器通过 Firestore `add` 自动生成文档 ID，因此它是追加写入，不会按用户指定的文档 ID 更新已有文档。

凭证可以通过 `credentials` 传入 Base64 编码后的服务账号 JSON；如果不配置该参数，则会从运行环境读取 Google 应用默认凭证。
该 sink 不会创建或管理 Firestore 索引；如果后续查询需要索引，请先在 Google Cloud 中创建。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源信息

使用 GoogleFirestore 连接器需要安装下面的依赖。可以通过 install-plugin.sh 安装，也可以从 Maven 中央仓库下载。

| 数据源          | 依赖 |
|-----------------|------|
| GoogleFirestore | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-google-firestore) |

## 选项

| 名称           | 类型   | 必填 | 默认值 | 说明 |
|----------------|--------|------|--------|------|
| project_id     | string | 是   | -      | Firestore 数据库所在的 Google Cloud 项目 ID。 |
| collection     | string | 是   | -      | 要写入的 Firestore 集合名称。 |
| credentials    | string | 否   | -      | Base64 编码后的 Google Cloud 服务账号 JSON。 |
| common-options |        | 否   | -      | Sink 通用选项，详见 [Sink 通用选项](../common-options/sink-common-options.md)。 |

### project_id [string]

Firestore 数据库所在的 Google Cloud 项目 ID。

### collection [string]

要写入的 Firestore 集合名称。每个 sink 配置块写入一个集合。

### credentials [string]

Base64 编码后的 Google Cloud 服务账号 JSON。

如果不配置该参数，连接器会使用 Google 应用默认凭证。此时需要确保 `GOOGLE_APPLICATION_CREDENTIALS` 指向服务账号 JSON 文件，或者运行环境已经提供默认凭证。

可以用下面的命令生成该配置值：

```bash
base64 -w 0 service-account.json
```

macOS 可以使用：

```bash
base64 service-account.json | tr -d '\n'
```

### 通用选项

Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md)。

## 支持的数据类型

| SeaTunnel 类型 | Firestore 值 |
|----------------|--------------|
| TINYINT        | 整数 |
| SMALLINT       | 整数 |
| INT            | 整数 |
| BIGINT         | 整数 |
| FLOAT          | 双精度浮点数 |
| DOUBLE         | 双精度浮点数 |
| DECIMAL        | 十进制数 |
| STRING         | 字符串 |
| BOOLEAN        | 布尔值 |
| BYTES          | 二进制 Blob |
| DATE           | UTC 当天零点日期 |
| TIMESTAMP      | 时间戳 |
| ARRAY          | 数组 |
| MAP            | Map |
| NULL           | null |

## 注意事项

- 当前连接器只提供 sink，不提供 GoogleFirestore source。
- 每个 sink 配置块只写入一个固定的 collection，不会按多表输入自动切换 collection；多 collection 场景需要为每个目标 collection 单独配置一个 sink 块。
- Firestore 文档 ID 会自动生成。如果需要固定文档 ID，请在写入前使用其他连接器或转换处理。
- sink 不会按 `UPDATE` 或 `DELETE` 行类型执行 CDC 语义 —— 每条记录都会触发一次 Firestore `add` 调用生成新文档。
- `credentials` 不能直接填写服务账号 JSON 原文，需要先做 Base64 编码。
- 上游 SeaTunnel schema 中的字段名会作为 Firestore 文档字段名。
- 连接器同时支持 `BATCH` 与 `STREAMING` 两种作业模式。在当前实现中，`FirestoreSinkWriter.write()` 对每一条记录直接调用一次 Firestore 客户端的 `add(...)`，并不会缓冲或批量写入，因此并没有“checkpoint 前把内存写缓冲刷到 Firestore”这一行为；checkpoint 完成并不意味着之前调用过的所有写入都已真正到达 Firestore。

## 任务示例

### 批量写入带类型的记录

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [{"a": "b"}, [10], "c_string", true, 117, 15987, 56387395, 7084913402530365000, 1.23, 1.23, "2924137191386439303744.39292216", null, "bWlJWmo=", "2023-04-22", "2023-04-22T23:20:58"]
      }
    ]
  }
}

sink {
  GoogleFirestore {
    project_id = "dummy-project"
    collection = "dummy-collection"
    credentials = "base64-service-account-json"
  }
}
```

### 流式写入并启用 Checkpoint 间隔

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  FakeSource {
    row.num = 100
    schema = {
      fields {
        c_string = string
        c_int = int
        c_timestamp = timestamp
      }
    }
    plugin_output = "firestore_stream"
  }
}

sink {
  GoogleFirestore {
    plugin_input = "firestore_stream"
    project_id = "my-gcp-project"
    collection = "events"
    credentials = "base64-service-account-json"
  }
}
```

## 变更日志

<ChangeLog />
