# Calcite 用户自定义函数

> Calcite Transform 插件的用户自定义函数 (UDF)

## 描述

使用 `CalciteUdf` SPI 扩展 [Calcite Transform](calcite.md) 的自定义标量函数。实现类通过 Java `ServiceLoader` 在运行时自动发现。

## UDF API

```java
package org.apache.seatunnel.transform.calcite.udf;

public interface CalciteUdf extends AutoCloseable {

    /**
     * SQL 函数名，如 "MY_UPPER"。
     * 查询时大小写不敏感。
     */
    String functionName();

    /** 初始化 UDF 资源。在第一次 eval 之前调用一次。 */
    default void open() {}

    /** 释放 UDF 资源。 */
    @Override
    default void close() throws Exception {}
}
```

## UDF 实现示例

### 第一步：添加 Maven 依赖

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-transforms-v2</artifactId>
        <version>${seatunnel.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-api</artifactId>
        <version>${seatunnel.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>com.google.auto.service</groupId>
        <artifactId>auto-service</artifactId>
        <version>1.1.1</version>
        <scope>provided</scope>
    </dependency>
</dependencies>
```

### 第二步：实现 CalciteUdf

创建一个实现 `CalciteUdf` 接口的类，并添加 **public static `eval`** 方法：

```java
package com.example;

import org.apache.seatunnel.transform.calcite.udf.CalciteUdf;
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

- `eval` **必须是 `public static`** -- Calcite 代码生成直接调用静态方法，不创建实例。实例方法会导致 Calcite 每次创建新对象，绕过 `open()` 中的初始化。
- `eval` 方法签名决定 SQL 函数的输入/输出类型。例如 `String eval(String, int)` 表示 SQL 函数接受 VARCHAR 和 INTEGER 参数，返回 VARCHAR。
- `@AutoService(CalciteUdf.class)` 自动生成 `META-INF/services` 文件用于 SPI 发现。
- `functionName()` 返回 SQL 函数名。函数名**大小写不敏感** -- `MY_UPPER(...)`、`my_upper(...)`、`My_Upper(...)` 均可使用。

### 第三步：部署

构建 JAR 并放入 `${SEATUNNEL_HOME}/lib/`。如果 UDF 依赖第三方库，也需要一并放入该目录。

如果使用集群模式，需要将 JAR 放到所有节点的 `${SEATUNNEL_HOME}/lib/` 并重启集群。

### 第四步：在 SQL 中使用

```sql
SELECT MY_UPPER(name) AS upper_name FROM source_table
```

## 带生命周期的 UDF 示例

如果 UDF 需要初始化或释放资源（如数据库连接、缓存），可以覆写 `open()` 和 `close()`：

```java
package com.example;

import org.apache.seatunnel.transform.calcite.udf.CalciteUdf;
import com.google.auto.service.AutoService;

@AutoService(CalciteUdf.class)
public class PrefixUdf implements CalciteUdf {

    private static volatile String prefix;

    @Override
    public String functionName() {
        return "WITH_PREFIX";
    }

    @Override
    public void open() {
        prefix = "HELLO";
    }

    public static String eval(String input) {
        if (input == null) return null;
        String p = prefix;
        return (p != null ? p : "") + ": " + input;
    }

    @Override
    public void close() {
        prefix = null;
    }
}
```

:::caution

由于 `eval` 必须是静态方法，共享状态（如上面的 `prefix`）必须存储在静态字段中。对于简单引用类型请使用 `volatile`，对于复杂可变状态请使用适当的同步机制，以确保跨线程的可见性。

:::

## 上下文感知 UDF 示例

如果 UDF 需要访问行级元数据（如 CDC 场景的 RowKind、表路径等），可以在静态 `eval` 方法中通过 `CalciteUdfContext.current()` 获取：

```java
package com.example;

import org.apache.seatunnel.transform.calcite.udf.CalciteUdf;
import org.apache.seatunnel.transform.calcite.udf.CalciteUdfContext;
import com.google.auto.service.AutoService;

@AutoService(CalciteUdf.class)
public class RowKindUdf implements CalciteUdf {

    @Override
    public String functionName() {
        return "ROW_KIND";
    }

    public static String eval(String input) {
        CalciteUdfContext ctx = CalciteUdfContext.current();
        if (ctx == null || input == null) return null;
        return ctx.getRowKind().shortString() + ":" + input;
    }
}
```

`CalciteUdfContext` 提供以下方法：

| 方法 | 返回类型 | 说明 |
|------|---------|------|
| `getRawTableId()` | String | 原始表标识符（如 `db.schema.table`） |
| `getDatabase()` | String | 解析后的数据库名 |
| `getSchema()` | String | 解析后的 Schema 名 |
| `getTable()` | String | 解析后的表名 |
| `getRowKind()` | RowKind | 行变更类型：`INSERT`、`UPDATE_BEFORE`、`UPDATE_AFTER`、`DELETE` |

使用：

```sql
SELECT ROW_KIND(name) AS kind_name FROM source_table
```

## 多参数 UDF 示例

`eval` 方法可以接受多个不同类型的参数：

```java
package com.example;

import org.apache.seatunnel.transform.calcite.udf.CalciteUdf;
import com.google.auto.service.AutoService;

@AutoService(CalciteUdf.class)
public class SubstringUdf implements CalciteUdf {

    @Override
    public String functionName() {
        return "MY_SUBSTR";
    }

    public static String eval(String input, int start, int length) {
        if (input == null) return null;
        int end = Math.min(start + length, input.length());
        return input.substring(Math.max(0, start), end);
    }
}
```

使用：

```sql
SELECT MY_SUBSTR(name, 0, 3) AS short_name FROM source_table
```

## 完整作业示例

输入：

| id | name | age |
|----|------|-----|
| 1 | Joy Ding | 20 |
| 2 | May Ding | 21 |
| 3 | Kin Dom | 24 |
| 4 | Joy Dom | 22 |

```hocon
transform {
  Calcite {
    plugin_input = "fake"
    plugin_output = "result"
    sql = "SELECT id, MY_UPPER(name) AS name, age FROM fake"
  }
}
```

输出：

| id | name | age |
|----|------|-----|
| 1 | JOY DING | 20 |
| 2 | MAY DING | 21 |
| 3 | KIN DOM | 24 |
| 4 | JOY DOM | 22 |

## 类型映射

`eval` 方法的 Java 类型与 SQL 类型的对应关系：

| Java 类型 | SQL 类型 |
|-----------|----------|
| `String` | VARCHAR |
| `int` / `Integer` | INTEGER |
| `long` / `Long` | BIGINT |
| `float` / `Float` | REAL |
| `double` / `Double` | DOUBLE |
| `boolean` / `Boolean` | BOOLEAN |
| `java.math.BigDecimal` | DECIMAL |
| `byte[]` | VARBINARY |

## 更新日志

### next-release

- 添加 Calcite UDF 文档
