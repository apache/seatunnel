# Calcite

> Calcite SQL Transform 插件

## 描述

基于 [Apache Calcite](https://calcite.apache.org/) 的 SQL Transform 插件。使用标准 SQL 对数据行进行转换，在作业启动时编译 SQL 执行计划，运行时逐行应用。

:::tip

- 每行独立处理——不支持 `JOIN` 和跨行聚合（`GROUP BY`、`SUM`、`COUNT`）。
- 向量类型（FLOAT_VECTOR、BINARY_VECTOR 等）内部映射为 VARBINARY。请使用内置向量 UDF（如 `COSINE_DISTANCE`、`VECTOR_REDUCE`）进行向量运算。

:::

## 属性

| 名称 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| sql | string | 是 | - | 要执行的 SQL 语句 |
| table_transform | list | 否 | [] | 多表 CDC 场景下的逐表 SQL 覆盖 |
| table_match_regex | string | 否 | .* | 表路径匹配正则。不匹配的表直接透传 |
| row_error_handle_way | enum | 否 | FAIL | 行级错误处理方式：`FAIL`、`SKIP`、`ROUTE_TO_TABLE` |

### sql [string]

```hocon
sql = "SELECT id, UPPER(name) AS name, age + 1 AS next_age FROM source_table WHERE age > 18"
```

### table_transform [list]

多表 CDC 场景下的逐表 SQL 覆盖。每项指定 `table_path` 和 `sql`。未列出的表会回退到全局 `sql`（如果路径匹配 `table_match_regex`），否则直接透传。

```hocon
table_transform = [
  {
    table_path = "db.users"
    sql = "SELECT id, name, UPPER(email) AS email FROM users"
  },
  {
    table_path = "db.orders"
    sql = "SELECT order_id, amount * 100 AS amount_cents FROM orders"
  }
]
```

### table_match_regex [string]

用于过滤需要转换的表的正则表达式。只有路径匹配此正则的表才会应用全局 `sql`。不匹配的表直接透传。默认 `.*`（匹配所有）。

### row_error_handle_way [enum]

行级 SQL 执行错误的处理方式：

- `FAIL`（默认）-- 立即终止作业
- `SKIP` -- 跳过错误行，继续处理
- `ROUTE_TO_TABLE` -- 将错误行路由到独立的错误表

### 公共参数 [string]

Transform 公共参数，请参考 [Transform 插件公共参数](common-options/common-options.md)。

## 内置 UDF

所有内置 UDF 在任意必需参数为 `NULL` 时返回 `NULL`。
函数名大小写不敏感。例如 `MASK(...)`、`mask(...)`、`Mask(...)` 等价。

### 数据脱敏函数

#### MASK

```MASK(value, start, end, maskChar) -> STRING```

将 `[start, end)` 范围内的字符替换为 `maskChar`。范围无效时返回原值。maskChar 为 null 或空时默认 `*`。

示例：

```sql
SELECT MASK(phone, 3, 7, '*') AS masked_phone FROM t
```

#### MASK_HASH

```MASK_HASH(value) -> STRING```

返回输入的 SHA-256 十六进制哈希（64 字符）。确定性——相同输入总是产生相同哈希。

示例：

```sql
SELECT MASK_HASH(phone) AS phone_hash FROM t
```

#### DES_ENCRYPT

```DES_ENCRYPT(password, data) -> STRING```

使用 `password`（不少于 8 字符）对 `data` 进行 DES 加密（CBC/PKCS5Padding），返回 Base64 编码密文。

示例：

```sql
SELECT DES_ENCRYPT('12345678', secret) AS encrypted FROM t
```

#### DES_DECRYPT

```DES_DECRYPT(password, data) -> STRING```

使用相同密码解密 Base64 编码的密文。

示例：

```sql
SELECT DES_DECRYPT('12345678', encrypted_secret) AS original FROM t
```

### 向量函数

#### COSINE_DISTANCE

```COSINE_DISTANCE(vector1, vector2) -> DOUBLE```

返回介于 0 和 1 之间的 DOUBLE 值：0 表示完全相同的向量，1 表示完全正交的向量。

示例：

```sql
SELECT COSINE_DISTANCE(vec1, vec2) AS distance FROM t
```

#### L1_DISTANCE

```L1_DISTANCE(vector1, vector2) -> DOUBLE```

计算两个向量之间的曼哈顿（L1）距离。

示例：

```sql
SELECT L1_DISTANCE(vec1, vec2) AS dist FROM t
```

#### L2_DISTANCE

```L2_DISTANCE(vector1, vector2) -> DOUBLE```

计算两个向量之间的欧几里得（L2）距离。

示例：

```sql
SELECT L2_DISTANCE(vec1, vec2) AS dist FROM t
```

#### VECTOR_DIMS

```VECTOR_DIMS(vector) -> INT```

返回一个 INT 值，表示向量中的维数（元素数量）。

示例：

```sql
SELECT VECTOR_DIMS(embedding) AS dims FROM t
```

#### VECTOR_NORM

```VECTOR_NORM(vector) -> DOUBLE```

计算向量的 L2 范数（欧几里得范数），表示向量的长度或大小。

示例：

```sql
SELECT VECTOR_NORM(embedding) AS norm FROM t
```

#### INNER_PRODUCT

```INNER_PRODUCT(vector1, vector2) -> DOUBLE```

计算两个向量的内积（点积），用于测量向量之间的相似性和投影。

示例：

```sql
SELECT INNER_PRODUCT(vec1, vec2) AS ip FROM t
```

#### VECTOR_REDUCE

```VECTOR_REDUCE(vector_field, target_dimension, method)```

通用向量降维函数，支持多种降维方法。

**参数：**
- `vector_field`：要降维的向量字段（VECTOR 类型）
- `target_dimension`：目标维度（INTEGER，必须小于源维度）
- `method`：降维方法（STRING）：
  - **'TRUNCATE'**：截断法，保留前 N 个元素。最简单快速，但可能丢失被截断维度的信息。
  - **'RANDOM_PROJECTION'**：高斯随机投影法。在降维的同时保持向量间的相对距离，遵循 Johnson-Lindenstrauss 引理。
  - **'SPARSE_RANDOM_PROJECTION'**：稀疏随机投影法，矩阵元素大多为零。比常规随机投影更高效。

**返回值：** VARBINARY——降维后的向量

**示例：**

```sql
SELECT id, VECTOR_REDUCE(embedding, 256, 'TRUNCATE') AS reduced FROM t
SELECT id, VECTOR_REDUCE(embedding, 128, 'RANDOM_PROJECTION') AS reduced FROM t
SELECT id, VECTOR_REDUCE(embedding, 64, 'SPARSE_RANDOM_PROJECTION') AS reduced FROM t
```

#### VECTOR_NORMALIZE

```VECTOR_NORMALIZE(vector_field)```

将向量归一化为单位长度（模长 = 1）。对于计算余弦相似度很有用。

**参数：**
- `vector_field`：要归一化的向量字段（VECTOR 类型）

**返回值：** VARBINARY——归一化后的向量

**示例：**

```sql
SELECT id, VECTOR_NORMALIZE(embedding) AS unit_vec FROM t
```

除上述 UDF 外，Apache Calcite 提供的所有标准 SQL 函数均可使用（字符串、数学、日期/时间、JSON、条件表达式等）。完整函数参考请见 [Apache Calcite SQL 参考文档](https://calcite.apache.org/docs/reference.html)。

## 示例

### 基础 SELECT + WHERE

从 Source 读取的数据如下：

| id | name | age |
|----|------|-----|
| 1 | Joy Ding | 20 |
| 2 | May Ding | 21 |
| 3 | Kin Dom | 24 |
| 4 | Joy Dom | 15 |

```hocon
transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, name, age FROM fake WHERE age >= 18"
  }
}
```

结果表 `result` 中的数据为：

| id | name | age |
|----|------|-----|
| 1 | Joy Ding | 20 |
| 2 | May Ding | 21 |
| 3 | Kin Dom | 24 |

`age = 15` 的行被过滤。

### 字符串和数学函数

输入：

| id | name | salary |
|----|------|--------|
| 1 | Joy Ding | 5000 |
| 2 | May Ding | 8000 |

```hocon
transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, UPPER(name) AS name_upper, CHAR_LENGTH(name) AS name_len, salary * 1.1 AS new_salary FROM fake"
  }
}
```

输出：

| id | name_upper | name_len | new_salary |
|----|------------|----------|------------|
| 1 | JOY DING | 8 | 5500.0 |
| 2 | MAY DING | 8 | 8800.0 |

### CASE WHEN 条件表达式

输入：

| id | name | age |
|----|------|-----|
| 1 | Alice | 8 |
| 2 | Bob | 15 |
| 3 | Carol | 30 |
| 4 | Dave | 70 |

```hocon
transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, name, CASE WHEN age < 13 THEN 'child' WHEN age < 18 THEN 'teen' WHEN age < 65 THEN 'adult' ELSE 'senior' END AS age_group FROM fake"
  }
}
```

输出：

| id | name | age_group |
|----|------|-----------|
| 1 | Alice | child |
| 2 | Bob | teen |
| 3 | Carol | adult |
| 4 | Dave | senior |

### JSON 提取

输入：

| id | payload |
|----|---------|
| 1 | {"user": {"name": "Joy Ding", "email": "joy@example.com"}} |
| 2 | {"user": {"name": "May Ding", "email": "may@example.com"}} |

```hocon
transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, JSON_VALUE(payload, '$.user.name') AS user_name, JSON_VALUE(payload, '$.user.email') AS email FROM fake"
  }
}
```

输出：

| id | user_name | email |
|----|-----------|-------|
| 1 | Joy Ding | joy@example.com |
| 2 | May Ding | may@example.com |

### 数据脱敏（MASK + MASK_HASH + DES）

输入：

| id | phone | secret |
|----|-------|--------|
| 1 | 13812345678 | seatunnel-password |
| 2 | 13987654321 | connector-api-key |

```hocon
transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, MASK(phone, 3, 7, '*') AS masked_phone, MASK_HASH(phone) AS phone_hash, DES_ENCRYPT('12345678', secret) AS encrypted_secret FROM fake"
  }
}
```

输出：

| id | masked_phone | phone_hash | encrypted_secret |
|----|--------------|------------|------------------|
| 1 | 138\*\*\*\*5678 | a1b2c3...（64 字符 SHA-256 hex） | Base64 编码密文 |
| 2 | 139\*\*\*\*4321 | d4e5f6...（64 字符 SHA-256 hex） | Base64 编码密文 |

后续解密：

```hocon
transform {
  Calcite {
    plugin_input = "result"
    plugin_output = "decrypted"
    sql = "SELECT id, DES_DECRYPT('12345678', encrypted_secret) AS original_secret FROM result"
  }
}
```

### 向量运算

使用内置向量 UDF 在数据管线中计算距离、降维或归一化（例如 Milvus/Qdrant 源与目标之间的处理）。

```hocon
transform {
  Calcite {
    plugin_input = "vector_source"
    plugin_output = "result"
    sql = "SELECT id, COSINE_DISTANCE(query_vec, doc_vec) AS distance, VECTOR_DIMS(doc_vec) AS dims, VECTOR_REDUCE(doc_vec, 128, 'TRUNCATE') AS reduced_vec FROM vector_source"
  }
}
```

给定两个 FLOAT_VECTOR 列 `query_vec` 和 `doc_vec`，此配置计算余弦距离、提取维度，并将 `doc_vec` 从原始维度降至 128 维。

### 多表 CDC（table_transform）

```hocon
transform {
  Calcite {
    plugin_input = "cdc_source"
    plugin_output = "result"
    table_transform = [
      {
        table_path = "db.users"
        sql = "SELECT id, name, UPPER(email) AS email FROM users"
      },
      {
        table_path = "db.orders"
        sql = "SELECT order_id, amount * 100 AS amount_cents FROM orders"
      }
    ]
  }
}
```

未列入 `table_transform` 但匹配 `table_match_regex`（默认 `.*`）的表会应用全局 `sql`。不匹配任何规则的表直接透传。

### 错误处理（row_error_handle_way）

```hocon
transform {
  Calcite {
    plugin_input = "source_table"
    plugin_output = "result"
    sql = "SELECT id, CAST(age AS VARCHAR) AS age_str FROM source_table"
    row_error_handle_way = "SKIP"
  }
}
```

行级 SQL 执行出错时：

- `FAIL` -- 立即终止作业（默认，推荐用于数据质量要求高的场景）
- `SKIP` -- 静默跳过错误行
- `ROUTE_TO_TABLE` -- 将错误行路由到独立错误表，便于后续排查

## 自定义 UDF

通过 `CalciteUdf` SPI 添加自定义标量函数。完整的开发指南、API 参考、示例和类型映射请参阅 [Calcite 用户自定义函数](calcite-udf.md)。

## 限制

| 限制 | 说明 |
|------|------|
| 单表输入 | 每个 Transform 只注册一张表到 Calcite Schema，不支持多表 `JOIN` |
| 逐行处理 | 每行独立处理。`GROUP BY` / `SUM()` / `COUNT()` 作用于单行，通常无实际聚合意义 |
| WHERE 过滤 | `WHERE` 条件为 `false` 的行会被丢弃（不透传） |
| 表名匹配 | SQL `FROM` 中的表名必须与 `plugin_input` 值完全一致 |
| 仅标量 UDF | 仅支持标量函数，不支持表值函数和聚合 UDF |
| 向量类型映射 | 向量类型内部映射为 VARBINARY。可使用内置向量 UDF（COSINE_DISTANCE、L1_DISTANCE 等）进行向量运算 |

:::tip CDC Schema 变更
收到 `AlterTableEvent`（例如加列、删列）时，引擎会自动重建 SQL 执行计划并重新推导输出 Schema，无需手动干预。
:::

## 作业配置示例

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        phone = "string"
      }
    }
  }
}

transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, UPPER(name) AS name, age + 1 AS age, MASK(phone, 3, 7, '*') AS phone FROM fake WHERE age >= 0"
  }
}

sink {
  Console {
    plugin_input = "result"
  }
}
```

## 更新日志

### next-release

- 新增 Calcite Transform 插件
