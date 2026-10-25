# SQL用户定义函数

> SQL 转换插件的用户定义函数 (UDF)

## 描述

使用UDF SPI扩展SQL转换函数库。

## UDF API

```java
package org.apache.seatunnel.transform.sql.zeta;

public interface ZetaUDF {
    /**
     * Function name
     *
     * @return function name
     */
    String functionName();

    /**
     * The type of function result
     *
     * @param argsType input arguments type
     * @return result type
     */
    SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType);

    /**
     * Evaluate
     *
     * @param args input arguments
     * @return result value
     */
    Object evaluate(List<Object> args);

    /**
     * 是否需要行级上下文。
     */
    default boolean requiresContext() {
        return false;
    }

    /**
     * 带上下文执行。
     */
    default Object evaluateWithContext(List<Object> args, ZetaUDFContext context) {
        return evaluate(args);
    }

    /**
     * 初始化 UDF 资源。
     */
    default void open() throws Exception {}

    /**
     * 释放 UDF 资源。
     */
    default void close() {}
}
```

`ZetaUDFContext` 提供运行时行级元数据与字段：

- `getRawTableId()`
- `getDatabase()`
- `getSchema()`
- `getTable()`
- `getRowKind()`
- `getAllFields()`

说明：

- `database/schema/table` 的解析语义与 `TablePath.of(tableId)` 保持一致。
- 如果 `tableId` 格式不被支持，访问 `database/schema/table` 时会抛出 `IllegalArgumentException`。
- 已有 UDF 保持向后兼容，仍可只实现 `evaluate(List<Object> args)`。

## UDF 实现示例

将这些依赖项添加到您的 Maven 项目，并使用 provided 作用域。**依赖版本应与运行环境一致。**

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
        <version>1.0.1</version>
        <scope>provided</scope>
    </dependency>
</dependencies>

```

添加一个 Java 类来实现 ZetaUDF，类似于以下的方式：

```java

@AutoService(ZetaUDF.class)
public class ExampleUDF implements ZetaUDF {
    @Override
    public String functionName() {
        return "EXAMPLE";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) return null;
        return "UDF: " + arg;
    }
}
```

打包UDF项目并将jar文件复制到路径：${SEATUNNEL_HOME}/lib

## 支持上下文与生命周期的 UDF 示例

```java
@AutoService(ZetaUDF.class)
public class ContextLifecycleUdf implements ZetaUDF {

    private transient String prefix;

    @Override
    public String functionName() {
        return "CTX_LIFE";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public boolean requiresContext() {
        return true;
    }

    @Override
    public void open() {
        this.prefix = "OPENED";
    }

    @Override
    public Object evaluateWithContext(List<Object> args, ZetaUDFContext context) {
        String arg = args.get(0) == null ? null : String.valueOf(args.get(0));
        if (arg == null) {
            return null;
        }
        return prefix + ":" + context.getRowKind().shortString() + ":" + arg;
    }

    @Override
    public void close() {
        this.prefix = null;
    }
}
```

## 示例

源端数据读取的表格如下：

| id |   name   | age |
|----|----------|-----|
| 1  | Joy Ding | 20  |
| 2  | May Ding | 21  |
| 3  | Kin Dom  | 24  |
| 4  | Joy Dom  | 22  |

我们使用SQL查询中的UDF来转换源数据，类似于以下方式：

```
transform {
  Sql {
    plugin_input = "fake"
    plugin_output = "fake1"
    query = "select id, example(name) as name, age from dual"
  }
}
```

那么结果表 `fake1` 中的数据将会更新为

| id |     name      | age |
|----|---------------|-----|
| 1  | UDF: Joy Ding | 20  |
| 2  | UDF: May Ding | 21  |
| 3  | UDF: Kin Dom  | 24  |
| 4  | UDF: Joy Dom  | 22  |

## 更新日志

### 新版本

- 添加SQL转换连接器的UDF