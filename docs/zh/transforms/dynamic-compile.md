# DynamicCompile

> 动态编译 Java、Groovy 或 Scala 代码，新增或替换行字段。

## 支持的引擎

SeaTunnel Zeta、Flink 和 Spark。Scala 编译仅支持 Zeta。

## 描述

DynamicCompile 使用用户提供的源码定义输出列并计算字段值。未被替换的输入列会保留，新列追加到输出表结构。当现有转换插件无法表达所需的行计算时，可以使用此插件。代码在作业进程中执行，复杂计算或远程调用可能降低吞吐量。

:::caution 仅执行可信代码
插件以 SeaTunnel 进程的权限执行代码，不提供沙箱隔离。请限制源码提交和源码文件的修改权限，并在运行作业前审核代码。
:::

## 主要特性

- 编译内联源码，或从绝对路径读取源码文件。
- 根据列名新增列或替换现有列。
- 对多张表应用公共规则，或配置完整的单表规则。

## 选项

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| compile_language | Enum | 每个已配置的规则必填 | - | `JAVA`、`GROOVY` 或 `SCALA`；Scala 需要 Zeta。 |
| compile_pattern | Enum | 否 | `SOURCE_CODE` | 选择内联源码或源码文件。 |
| source_code | String | 条件必填 | - | `SOURCE_CODE` 模式下必须提供非空白源码，省略 `compile_pattern` 时同样必填。 |
| absolute_path | String | 条件必填 | - | `ABSOLUTE_PATH` 模式下必须提供非空白路径。 |
| table_match_regex | String | 否 | `.*` | 选择应用转换级别规则的表。 |
| table_transform | Array&lt;Map&gt; | 否 | `[]` | 包含精确 `table_path` 的完整单表规则。 |
| rule_match_mode | Enum | 否 | `FIRST_MATCH` | 对重复表路径使用首个规则或全部规则；显式配置要求见下文。 |
| common options | - | 否 | - | 参见[转换插件](common-options/common-options.md)。 |

### compile_language [Enum]

可选 `JAVA`、`GROOVY`、`SCALA`。每个已配置的规则都需要自己的语言设置。仅配置 `table_transform` 时，转换级别不需要设置语言。

Java 使用 Janino 的 `ClassBodyEvaluator`：源码应为 import 语句和 public 方法组成的类体，不要再包裹一层 class 声明。Janino 不支持当前 Java 语言的所有特性，参见 [Janino](https://github.com/janino-compiler/janino)。

Groovy 源码应定义包含所需 public 方法的类。Scala 源码应定义具有等效 JVM 方法签名的类，通过 Scala REPL 编译器进行编译，仅支持 Zeta。

### compile_pattern [Enum]

- `SOURCE_CODE`（默认）：编译 `source_code`，源码必须存在且不能全为空白。
- `ABSOLUTE_PATH`：读取并编译 `absolute_path` 指定的文件，路径必须存在且不能全为空白。

每个单表条目使用自己的模式，默认为 `SOURCE_CODE`，不会继承转换级别的模式。

### source_code [String]

内联源码，通常使用 HOCON 三引号字符串。实现以下两个 public 方法，并保持输出顺序一致：

```java
Column[] getInlineOutputColumns(CatalogTable inputCatalogTable)
Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor inputRow)
```

第一个方法定义输出列。对于现有列名，回调会替换行中的字段值，并可改变该列的数据类型；列的位置和其他元数据（例如是否可为空）保持不变。新名称的列按返回的列定义追加到表结构，其他输入列保持不变。

第二个方法计算单行的输出值。返回值数量必须与声明的输出列数一致，顺序和类型也必须匹配。通过 `SeaTunnelRowAccessor` 按输入表结构的字段索引读取输入值。

新代码请使用 `org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor`。如果源码需要第三方依赖，请将依赖部署到 Zeta 的 `${SEATUNNEL_HOME}/lib`，或对应 Flink/Spark 服务的库目录，并重启服务以加载依赖。

### absolute_path [String]

源码文件的绝对路径，在构造转换时读取。应确保所有初始化该转换的位置都可以通过该路径访问文件；提交机器上的本地文件不会自动分发。文件内容应满足与内联源码相同的语言和方法约定。修改文件不代表代码会热更新。

例如，将某个规则的内联源码设置替换为：

```hocon
compile_language = "JAVA"
compile_pattern = "ABSOLUTE_PATH"
absolute_path = "/opt/seatunnel/transforms/TagTransform.java"
```

### table_match_regex [String]

对完整输入表路径进行匹配的 Java 正则表达式，默认 `.*`。它选择应用已配置的转换级别规则的表，不限制匹配的 `table_transform` 条目，也不会在未配置时自动生成转换级别规则。

### table_transform [Array&lt;Map&gt;]

每个条目包含精确且非空白的 `table_path`，以及完整的编译规则：`compile_language`、可选的 `compile_pattern`，以及该模式要求的源码或路径。条目不会继承转换级别的编译设置。

匹配的条目优先于转换级别规则，即使该表不匹配 `table_match_regex`，条目仍然生效。

### rule_match_mode [Enum]

当多个条目具有相同的精确 `table_path` 时：

- 显式设置 `FIRST_MATCH`：按列表顺序使用首个条目。
- 显式设置 `ALL_MATCH`：按列表顺序应用全部条目。每个规则接收上一个规则产生的表结构和行。

虽然默认值为 `FIRST_MATCH`，但省略该选项时，转换初始化会拒绝重复表路径。如果确实需要重复路径，请显式设置该选项。

### common options

通过 `plugin_input` 和 `plugin_output` 连接其他插件。参见[转换插件](common-options/common-options.md)。

## 多表配置

每张输入表按以下顺序选择实际规则：

1. 匹配的 `table_transform` 条目。
2. 否则，使用已配置且被 `table_match_regex` 选中的转换级别规则。
3. 否则，表结构和行保持不变。

如提供转换级别规则，规则必须完整。仅配置 `table_transform` 时，未列出的表保持不变。参见[多表转换](transform-multi-table.md)。

## 示例

以下完整批作业配置使用两张 FakeSource 表，每张表只有一个 `id` 列。Java 代码保留 `id` 并新增常量 `tag` 列。Console 的行顺序和生成的 ID 可能变化，下文的预期表结构及标签不依赖这些值。

| 表 | 输入结构 | 仅转换级别规则 | 包含单表覆盖规则 | 仅单表规则 |
| --- | --- | --- | --- | --- |
| `demo.orders` | `id INT` | `id INT, tag STRING`（`default`） | `id INT, tag STRING`（`orders`） | `id INT, tag STRING`（`orders`） |
| `demo.returns` | `id INT` | `id INT, tag STRING`（`default`） | `id INT, tag STRING`（`default`） | `id INT`（不变） |

### 仅配置转换级别规则

`demo.orders` 和 `demo.returns` 均输出 `id, tag`，其中 `tag = "default"`。

```hocon
env { job.mode = "BATCH" }
source {
  FakeSource {
    plugin_output = "input"
    tables_configs = [
      { row.num = 1, schema = { table = "demo.orders", fields { id = "int" } } },
      { row.num = 1, schema = { table = "demo.returns", fields { id = "int" } } }
    ]
  }
}
transform {
  DynamicCompile {
    plugin_input = "input"
    plugin_output = "output"
    compile_language = "JAVA"
    source_code = """
      import org.apache.seatunnel.api.table.catalog.Column;
      import org.apache.seatunnel.api.table.catalog.CatalogTable;
      import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
      import org.apache.seatunnel.api.table.type.BasicType;
      import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;

      public Column[] getInlineOutputColumns(CatalogTable table) {
          return new Column[] {
              PhysicalColumn.of("tag", BasicType.STRING_TYPE, 20L, true, "", "")
          };
      }
      public Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor row) {
          return new Object[] {"default"};
      }
    """
  }
}
sink {
  Console { plugin_input = "output" }
}
```

### 转换级别规则与单表覆盖规则

`demo.orders` 的 `tag = "orders"`，`demo.returns` 的 `tag = "default"`。orders 的规则替代公共规则，而不是在公共规则之后再执行。

```hocon
env { job.mode = "BATCH" }
source {
  FakeSource {
    plugin_output = "input"
    tables_configs = [
      { row.num = 1, schema = { table = "demo.orders", fields { id = "int" } } },
      { row.num = 1, schema = { table = "demo.returns", fields { id = "int" } } }
    ]
  }
}
transform {
  DynamicCompile {
    plugin_input = "input"
    plugin_output = "output"
    table_match_regex = "demo[.].*"
    compile_language = "JAVA"
    source_code = """
      import org.apache.seatunnel.api.table.catalog.Column;
      import org.apache.seatunnel.api.table.catalog.CatalogTable;
      import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
      import org.apache.seatunnel.api.table.type.BasicType;
      import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;

      public Column[] getInlineOutputColumns(CatalogTable table) {
          return new Column[] {
              PhysicalColumn.of("tag", BasicType.STRING_TYPE, 20L, true, "", "")
          };
      }
      public Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor row) {
          return new Object[] {"default"};
      }
    """
    table_transform = [
      {
        table_path = "demo.orders"
        compile_language = "JAVA"
        source_code = """
          import org.apache.seatunnel.api.table.catalog.Column;
          import org.apache.seatunnel.api.table.catalog.CatalogTable;
          import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
          import org.apache.seatunnel.api.table.type.BasicType;
          import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;

          public Column[] getInlineOutputColumns(CatalogTable table) {
              return new Column[] {
                  PhysicalColumn.of("tag", BasicType.STRING_TYPE, 20L, true, "", "")
              };
          }
          public Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor row) {
              return new Object[] {"orders"};
          }
        """
      }
    ]
  }
}
sink {
  Console { plugin_input = "output" }
}
```

### 仅配置单表规则

只有 `demo.orders` 新增 `tag = "orders"`。`demo.returns` 保留原来的单列 `id` 表结构，即使默认的 `table_match_regex` 匹配该表也不会新增列。

```hocon
env { job.mode = "BATCH" }
source {
  FakeSource {
    plugin_output = "input"
    tables_configs = [
      { row.num = 1, schema = { table = "demo.orders", fields { id = "int" } } },
      { row.num = 1, schema = { table = "demo.returns", fields { id = "int" } } }
    ]
  }
}
transform {
  DynamicCompile {
    plugin_input = "input"
    plugin_output = "output"
    table_transform = [
      {
        table_path = "demo.orders"
        compile_language = "JAVA"
        source_code = """
          import org.apache.seatunnel.api.table.catalog.Column;
          import org.apache.seatunnel.api.table.catalog.CatalogTable;
          import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
          import org.apache.seatunnel.api.table.type.BasicType;
          import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;

          public Column[] getInlineOutputColumns(CatalogTable table) {
              return new Column[] {
                  PhysicalColumn.of("tag", BasicType.STRING_TYPE, 20L, true, "", "")
              };
          }
          public Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor row) {
              return new Object[] {"orders"};
          }
        """
      }
    ]
  }
}
sink {
  Console { plugin_input = "output" }
}
```

### Groovy 和 Scala 源码形式

使用 Groovy 时，将规则中的 `compile_language` 设为 `"GROOVY"`，并将 `source_code` 替换为：

```groovy
import org.apache.seatunnel.api.table.catalog.Column
import org.apache.seatunnel.api.table.catalog.CatalogTable
import org.apache.seatunnel.api.table.catalog.PhysicalColumn
import org.apache.seatunnel.api.table.type.BasicType
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor

class GroovyTag {
    public Column[] getInlineOutputColumns(CatalogTable table) {
        return [PhysicalColumn.of("tag", BasicType.STRING_TYPE, 20L, true, "", "")] as Column[]
    }
    public Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor row) {
        return ["GROOVY"] as Object[]
    }
}
```

在 Zeta 上使用 Scala 时，将 `compile_language` 设为 `"SCALA"`，并使用：

```scala
import org.apache.seatunnel.api.table.catalog.{Column, CatalogTable, PhysicalColumn}
import org.apache.seatunnel.api.table.`type`.{BasicType, SeaTunnelRowAccessor}

class ScalaTag {
  def getInlineOutputColumns(table: CatalogTable): Array[Column] = {
    Array[Column](PhysicalColumn.of("tag", BasicType.STRING_TYPE, 20L, true, "", ""))
  }
  def getInlineOutputFieldValues(row: SeaTunnelRowAccessor): Array[Object] = {
    Array[Object]("SCALA")
  }
}
```

两个版本均保留 `id`，并新增值为语言名称的 `tag`。更多配置可参考 [DynamicCompile E2E 配置](https://github.com/apache/seatunnel/tree/dev/seatunnel-e2e/seatunnel-transforms-v2-e2e/seatunnel-transforms-v2-e2e-part-2/src/test/resources/dynamic_compile/conf)。

## Changelog
