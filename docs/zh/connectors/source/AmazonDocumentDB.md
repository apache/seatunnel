import ChangeLog from '../changelog/connector-amazondocumentdb.md';

# AmazonDocumentDB

<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

> Amazon DocumentDB 源连接器

## 连接器支持版本

- 兼容 MongoDB 4.0 和 5.0 API 的 Amazon DocumentDB 集群

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 描述

通过有界批扫描从已有 Amazon DocumentDB 集合读取文档。

本连接器 V1 范围：

- 仅支持 source
- 每个 source 读取一个 database 和一个 collection
- 必须显式配置 schema，不支持 schema 推断
- 支持可选 BSON 过滤和投影
- 仅使用一个 source split，不支持采样或按范围并行分片
- sink、CDC/change stream、catalog 发现和集合创建均不在范围内

## 支持的数据源信息

可通过 `install-plugin.sh` 安装连接器，或从 Maven Central 下载。

| 数据源 | 支持版本 | 依赖 |
| --- | --- | --- |
| Amazon DocumentDB | 兼容 MongoDB 4.0 和 5.0 | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-amazondocumentdb) |

## 数据库依赖

> 运行作业前请先安装连接器插件：

```shell
sh bin/install-plugin.sh ${version}
```

请确保插件安装列表包含 `connector-amazondocumentdb`。连接器使用 MongoDB Java 同步驱动 4.7.1。Amazon DocumentDB 通常只能从集群所在 VPC 或与该 VPC 打通网络的环境访问。

## 数据类型映射

必须显式定义 SeaTunnel schema。连接器根据配置的字段类型转换 BSON 值：

| Amazon DocumentDB BSON 类型 | SeaTunnel 数据类型 |
| --- | --- |
| Boolean | BOOLEAN |
| Int32 / Int64 / Double | TINYINT / SMALLINT / INT / BIGINT / FLOAT / DOUBLE |
| Decimal128 | DECIMAL |
| String / ObjectId / Document | STRING |
| Date / Timestamp | DATE / TIME / TIMESTAMP |
| Binary | BYTES |
| Array | ARRAY |
| Document | MAP / ROW |
| Null / Undefined / Decimal128 NaN | null |

Decimal128 值会按配置的 scale 四舍五入。如果结果精度超过声明的 DECIMAL precision，转换将报错，
而不会静默产生 `null`。

## 源配置项

| 名称 | 类型 | 必需 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| uri | string | 是 | - | 包含 Amazon DocumentDB endpoint 和凭据的 MongoDB 兼容连接 URI |
| database | string | 是 | - | 数据库名称 |
| collection | string | 是 | - | 集合名称 |
| schema | config | 是 | - | 显式数据 schema |
| tls | boolean | 否 | true | 是否启用 TLS |
| tls_ca_file | string | `tls=true` 时是 | - | 用于验证集群证书的 PEM CA bundle 本地路径 |
| match.query | string | 否 | `{}` | BSON/JSON 过滤文档 |
| match.projection | string | 否 | - | BSON/JSON 投影文档 |
| fetch.size | int | 否 | 2048 | 每批向服务端请求的文档数，必须大于零 |
| common-options | - | 否 | - | Source 插件通用参数，详见 [Source Common Options](../common-options/source-common-options.md) |

### uri [string]

MongoDB 兼容连接 URI。请包含用户名、密码、集群 endpoint 和端口。Amazon DocumentDB 不支持 retryable writes，因此连接器会拒绝显式的 `retryWrites=true`，并在解析 URI 后始终应用 `retryWrites=false`。

示例：`mongodb://reader:<password>@sample.cluster-abcdefghijkl.us-east-1.docdb.amazonaws.com:27017/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false`

### database [string]

要读取的已有 Amazon DocumentDB 数据库名称。

### collection [string]

要读取的已有集合名称。

### schema [config]

显式 SeaTunnel schema。连接器按字段名从每个 BSON 文档取值；字段缺失或 BSON null 时输出 null。

```hocon
schema = {
  fields {
    _id = string
    status = string
    amount = "decimal(18,2)"
    created_at = timestamp
    labels = "map<string,string>"
  }
}
```

### tls [boolean]

是否启用 TLS，默认值为 `true`。连接器在解析 `uri` 后应用该配置，因此它决定驱动最终的 TLS 设置。

### tls_ca_file [string]

可读的 PEM CA bundle 路径。`tls=true` 时必须配置。连接器会构造自己专用的 `SSLContext`，不会修改 JVM 全局 `javax.net.ssl.trustStore` 系统属性。

请从 [AWS 文档](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect_programmatically.html)下载当前 Amazon trust store。

### match.query [string]

传给 `find` 的 BSON/JSON 过滤条件，例如 `{"status": "OPEN"}`。默认 `{}` 读取全部文档。

### match.projection [string]

传给 `find` 的 BSON/JSON 投影，例如 `{"_id": 1, "status": 1, "amount": 1}`。

### fetch.size [int]

服务端游标的驱动批大小提示。增大该值可减少网络往返，但会增加驱动缓冲的数据量。

### common-options

Source 插件通用参数，详见 [Source Common Options](../common-options/source-common-options.md)。

### 提示

> 1. 建议使用只读 Amazon DocumentDB 用户，并避免把凭据提交到版本库中的作业文件。<br/>
> 2. TLS 默认启用。请使用当前 AWS CA bundle，并在 AWS 更新信任链后轮换本地文件。<br/>
> 3. V1 只创建一个 split，提高 source 并行度不会并行扫描集合。<br/>
> 4. Split 状态只保存过滤和投影，不保存游标进度。失败恢复会从集合开头重新执行完整扫描，即使上一次扫描已接近完成；因此下游写入必须具备幂等性，或采用 truncate-and-reload 策略，以避免重复数据。<br/>
> 5. 建议把高选择性条件放到 `match.query`，并在 `match.projection` 中包含 schema 所需的全部字段。

## 如何创建 Amazon DocumentDB 数据同步作业

下面的批处理作业从已有集合读取数据并打印到本地客户端：

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  AmazonDocumentDB {
    uri = "mongodb://reader:<password>@sample.cluster-abcdefghijkl.us-east-1.docdb.amazonaws.com:27017/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    database = "app-db"
    collection = "orders"
    tls = true
    tls_ca_file = "/opt/seatunnel/certs/global-bundle.pem"
    fetch.size = 2048
    schema = {
      fields {
        _id = string
        status = string
        amount = "decimal(18,2)"
        created_at = timestamp
      }
    }
  }
}

sink {
  Console {}
}
```

### 过滤和投影文档

```bash
source {
  AmazonDocumentDB {
    uri = "mongodb://reader:<password>@sample.cluster-abcdefghijkl.us-east-1.docdb.amazonaws.com:27017/?replicaSet=rs0&retryWrites=false"
    database = "app-db"
    collection = "orders"
    tls_ca_file = "/opt/seatunnel/certs/global-bundle.pem"
    match.query = '{"status": "OPEN", "amount": {"$gt": 100}}'
    match.projection = '{"_id": 1, "status": 1, "amount": 1}'
    fetch.size = 512
    schema = {
      fields {
        _id = string
        status = string
        amount = "decimal(18,2)"
      }
    }
  }
}
```

## 变更日志

<ChangeLog />
