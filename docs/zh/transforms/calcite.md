# Calcite

> Calcite SQL Transform 插件

## 描述

基于 [Apache Calcite](https://calcite.apache.org/) 的 SQL Transform 插件。使用标准 SQL 对数据行进行转换，在作业启动时编译 SQL 执行计划，运行时逐行应用。

## 属性

| 名称 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| sql | string | 是 | - | 要执行的 SQL 语句。`FROM` 表名必须与 `plugin_input` 一致 |
| table_transform | list | 否 | [] | 多表 CDC 场景下的逐表 SQL 覆盖 |
| table_match_regex | string | 否 | .* | 表路径匹配正则。不匹配的表直接透传 |
| row_error_handle_way | enum | 否 | FAIL | 行级错误处理方式：`FAIL`、`SKIP`、`ROUTE_TO_TABLE` |

### sql [string]

使用 Apache Calcite 引擎执行的 SQL 语句。`FROM` 中的表名必须与 `plugin_input` 值一致。

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

## 支持的数据类型

SeaTunnel 与 Calcite 之间的双向类型映射：

| SeaTunnel 类型 | Calcite 类型 | 说明 |
|---------------|-------------|------|
| BOOLEAN | BOOLEAN | |
| TINYINT | TINYINT | |
| SMALLINT | SMALLINT | |
| INT | INTEGER | |
| BIGINT | BIGINT | |
| FLOAT | REAL | |
| DOUBLE | DOUBLE | |
| DECIMAL(p,s) | DECIMAL(p,s) | 精度和标度保留 |
| STRING | VARCHAR | |
| BYTES | VARBINARY | |
| DATE | DATE | |
| TIME | TIME | |
| TIMESTAMP | TIMESTAMP | |
| TIMESTAMP_TZ | TIMESTAMP_WITH_LOCAL_TIME_ZONE | |
| NULL | NULL | |
| ARRAY | ARRAY | 元素类型递归映射 |
| MAP | MAP | 键值类型递归映射 |
| ROW | ROW（struct） | 字段名和类型保留 |
| BINARY_VECTOR | VARBINARY | 有损：向量语义在 SQL 中不保留 |
| FLOAT_VECTOR | VARBINARY | 有损：向量语义在 SQL 中不保留 |
| FLOAT16_VECTOR | VARBINARY | 有损：向量语义在 SQL 中不保留 |
| BFLOAT16_VECTOR | VARBINARY | 有损：向量语义在 SQL 中不保留 |
| SPARSE_FLOAT_VECTOR | VARBINARY | 有损：向量语义在 SQL 中不保留 |

Calcite 的 INTERVAL 类型（如 `INTERVAL YEAR`、`INTERVAL DAY TO SECOND`）在输出时映射为 `BIGINT`。

## 内置 UDF

| 函数 | 签名 | 返回类型 | 说明 |
|------|------|---------|------|
| MASK | `MASK(value, start, end, maskChar)` | STRING | 将 `[start, end)` 范围内的字符替换为 `maskChar`。范围无效时返回原值。maskChar 为 null 或空时默认 `*` |
| MASK_HASH | `MASK_HASH(value)` | STRING | 返回输入的 SHA-256 十六进制哈希（64 字符）。确定性——相同输入总是产生相同哈希 |
| DES_ENCRYPT | `DES_ENCRYPT(password, data)` | STRING | 使用 `password`（不少于 8 字符）对 `data` 进行 DES 加密（CBC/PKCS5Padding），返回 Base64 编码密文 |
| DES_DECRYPT | `DES_DECRYPT(password, data)` | STRING | 使用相同密码解密 Base64 编码的密文 |
| URL_ENCODE | `URL_ENCODE(value)` | STRING | 对输入字符串进行 URL 编码（UTF-8） |
| URL_DECODE | `URL_DECODE(value)` | STRING | 对输入字符串进行 URL 解码（UTF-8） |

所有内置 UDF 在任意必需参数为 `NULL` 时返回 `NULL`。

## 内置 SQL 函数

Calcite 提供 200+ 标准 SQL 函数，以下是常用分类：

### 字符串函数

`UPPER`、`LOWER`、`TRIM`、`CONCAT`、`SUBSTRING`、`REPLACE`、`CHAR_LENGTH`、`POSITION`、`OVERLAY`、`INITCAP`

### 数学函数

`ABS`、`MOD`、`POWER`、`SQRT`、`FLOOR`、`CEIL`、`ROUND`、`SIGN`、`LN`、`LOG10`、`EXP`

### 日期/时间函数

`CURRENT_DATE`、`CURRENT_TIMESTAMP`、`EXTRACT`、`TIMESTAMPADD`、`TIMESTAMPDIFF`、`YEAR`、`MONTH`、`DAYOFMONTH`

### JSON 函数

`JSON_VALUE(json, '$.path')`、`JSON_QUERY`、`JSON_EXISTS`

### 条件函数

`CASE WHEN ... THEN ... ELSE ... END`、`COALESCE`、`NULLIF`、`GREATEST`、`LEAST`

### 比较与逻辑运算

`=`、`<>`、`<`、`>`、`IN (...)`、`BETWEEN ... AND ...`、`LIKE`、`IS NULL`、`IS NOT NULL`、`AND`、`OR`、`NOT`

### 类型转换

`CAST(expr AS type)`

完整函数参考请见 [Apache Calcite SQL 参考文档](https://calcite.apache.org/docs/reference.html)。

> **注意：** Calcite Transform 逐行处理数据。聚合函数如 `SUM`、`COUNT`、`AVG` 语法上合法，但作用于单行，通常没有实际聚合意义。仅在窗口函数或特定单行场景中使用。

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

### CASE WHEN 条件转换

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

## 自定义 UDF 开发指南

通过 `CalciteUdf` SPI 添加自定义 UDF。实现接口，打包为 JAR，放入 `${SEATUNNEL_HOME}/lib/` 即可自动发现。

**第一步**：创建一个实现 `CalciteUdf` 接口的类，并添加 **public static `eval`** 方法：

```java
package com.example;

import org.apache.seatunnel.api.transform.CalciteUdf;
import com.google.auto.service.AutoService;
import java.util.Locale;

@AutoService(CalciteUdf.class)
public class MyUpperUdf implements CalciteUdf {

    @Override
    public String functionName() {
        return "MY_UPPER";
    }

    public static String eval(String input) {
        return input == null ? null : input.toUpperCase(Locale.ROOT);
    }
}
```

关键要求：
- `eval` **必须是 `public static`** -- Calcite 代码生成直接调用静态方法，不创建实例
- `eval` 方法签名决定 SQL 函数的输入/输出类型（如 `String eval(String, int)` 表示接受 VARCHAR 和 INTEGER）
- `@AutoService(CalciteUdf.class)` 自动生成 `META-INF/services` 文件用于 SPI 发现
- `functionName()` 返回 SQL 函数名（查询时大小写不敏感）

**第二步**：在 `pom.xml` 中添加 `auto-service` 依赖：

```xml
<dependency>
    <groupId>com.google.auto.service</groupId>
    <artifactId>auto-service</artifactId>
    <version>1.1.1</version>
    <scope>provided</scope>
</dependency>
```

**第三步**：构建 JAR 并放入 `${SEATUNNEL_HOME}/lib/`。

**第四步**：在 SQL 中使用：

```sql
SELECT MY_UPPER(name) AS upper_name FROM source_table
```

## 限制

| 限制 | 说明 |
|------|------|
| 单表输入 | 每个 Transform 只注册一张表到 Calcite Schema，不支持多表 `JOIN` |
| 逐行处理 | 每行独立处理。`GROUP BY` / `SUM()` / `COUNT()` 作用于单行，通常无实际聚合意义 |
| WHERE 过滤 | `WHERE` 条件为 `false` 的行会被丢弃（不透传） |
| 表名匹配 | SQL `FROM` 中的表名必须与 `plugin_input` 值完全一致 |
| 仅标量 UDF | 仅支持标量函数，不支持表值函数和聚合 UDF |
| 向量类型有损 | 向量类型（BINARY_VECTOR、FLOAT_VECTOR 等）映射为 VARBINARY，丢失向量语义 |

## FAQ

**Q：SQL 的 `FROM` 表名怎么写？**

A：必须与 `plugin_input` 值一致。例如 `plugin_input = "fake"` 时，SQL 应为 `SELECT ... FROM fake`。

**Q：UDF 函数名需要加引号吗？**

A：不需要。Calcite 引擎配置了大小写不敏感匹配，`MASK(...)`、`mask(...)`、`Mask(...)` 都可以。

**Q：支持 JOIN 吗？**

A：不支持。Calcite Transform 只注册一张输入表。跨表操作请串联多个 Transform 或使用其他方案。

**Q：可以用 GROUP BY 或聚合函数吗？**

A：语法上可以，但没有实际聚合意义。引擎逐行处理，`SUM(amount)` 只是返回该行的 `amount` 值。

**Q：自定义 UDF 的 `eval` 方法必须是 `static` 吗？**

A：是的。Calcite 代码生成直接调用静态方法。实例方法会导致 Calcite 每次创建新对象，绕过 `open()` 中的初始化。

**Q：如何扩展新的 UDF？**

A：实现 `CalciteUdf` SPI 并加 `@AutoService`，将 JAR 放入 `${SEATUNNEL_HOME}/lib/`。详见[自定义 UDF 开发指南](#自定义-udf-开发指南)。

**Q：CDC 场景下 Schema 变更怎么处理？**

A：收到 `AlterTableEvent`（如加列、删列）时，引擎自动重建 SQL 执行计划并重新推导输出 Schema。

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
