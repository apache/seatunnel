# DynamicCompile

> Compile custom Java, Groovy or Scala code to add or replace row fields.

## Support Those Engines

SeaTunnel Zeta, Flink and Spark. Scala compilation is supported only on Zeta.

## Description

DynamicCompile computes output columns and their values using user-supplied source code. It keeps input columns that are not replaced and appends new columns to the output schema. Use it when an existing transform cannot express the required row calculation. Code runs inside the job process; expensive calculations or remote calls can reduce throughput.

:::caution Trusted code only
This plugin executes code with the permissions of the SeaTunnel process. It is not a sandbox. Restrict who can submit source code or change a source file, and review that code before running the job.
:::

## Key Features

- Compile inline code or read it from an absolute file path.
- Add columns or replace existing columns by name.
- Apply a shared rule to multiple tables or select complete per-table rules.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| compile_language | Enum | Yes, in each configured rule | - | `JAVA`, `GROOVY` or `SCALA`; Scala requires Zeta. |
| compile_pattern | Enum | No | `SOURCE_CODE` | Read inline code or a source file. |
| source_code | String | Conditional | - | Nonblank code required for `SOURCE_CODE`, including when `compile_pattern` is omitted. |
| absolute_path | String | Conditional | - | Nonblank file path required for `ABSOLUTE_PATH`. |
| table_match_regex | String | No | `.*` | Select tables for the transform-level rule. |
| table_transform | Array&lt;Map&gt; | No | `[]` | Complete per-table rules with an exact `table_path`. |
| rule_match_mode | Enum | No | `FIRST_MATCH` | Select the first or all entries for a repeated table path; see the explicit-setting requirement below. |
| common options | - | No | - | See [Transform Plugin](common-options/common-options.md). |

### compile_language [Enum]

Choose `JAVA`, `GROOVY` or `SCALA`. Each configured rule needs its own language. A `table_transform`-only configuration does not require a transform-level language.

Java uses Janino's `ClassBodyEvaluator`: provide imports and public methods as a class body, without a surrounding class declaration. Janino does not support every feature of the current Java language; see [Janino](https://github.com/janino-compiler/janino).

Groovy code defines a class containing the required public methods. Scala code defines a class with equivalent JVM method signatures and uses the Scala REPL compiler; it is supported only on Zeta.

### compile_pattern [Enum]

- `SOURCE_CODE` (default): compile `source_code`; it must be present and nonblank.
- `ABSOLUTE_PATH`: read and compile the file at `absolute_path`; the path must be present and nonblank.

Each per-table entry uses its own pattern, defaulting to `SOURCE_CODE`. It does not inherit the transform-level pattern.

### source_code [String]

Inline source code, usually enclosed in a HOCON triple-quoted string. Implement these two public methods, with matching output order:

```java
Column[] getInlineOutputColumns(CatalogTable inputCatalogTable)
Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor inputRow)
```

The first method defines the columns to produce. For an existing name, the callback replaces the row value and may change the column's data type; the column keeps its position and other metadata, such as nullability. A new name appends a column using the returned definition. Other input columns remain.

The second method computes the values for one row. Return one value per declared output column, in the same order and with compatible types. Read input fields by their input-schema indexes through `SeaTunnelRowAccessor`.

Use `org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor` in new code. If the code needs third-party libraries, make them available in `${SEATUNNEL_HOME}/lib` on Zeta, or the corresponding Flink/Spark service library directory, and restart the service to load them.

### absolute_path [String]

An absolute path to the source file, read when the transform is constructed. Make the file available at that path wherever the transform is initialized; a file present only on the submission machine is not automatically distributed. The file must follow the same language and method contract as inline code. Changing it is not a hot-reload mechanism.

For example, replace the inline-code settings of a rule with:

```hocon
compile_language = "JAVA"
compile_pattern = "ABSOLUTE_PATH"
absolute_path = "/opt/seatunnel/transforms/TagTransform.java"
```

### table_match_regex [String]

A Java regular expression matched against the whole input table path. It selects tables for a configured transform-level rule and defaults to `.*`. It neither limits matching `table_transform` entries nor creates a transform-level rule when none is configured.

### table_transform [Array&lt;Map&gt;]

Each entry contains an exact, nonblank `table_path` and a complete compilation rule: `compile_language`, optional `compile_pattern`, and the code or path required by that pattern. Entries do not inherit compilation settings from the transform level.

A matching entry takes precedence over the transform-level rule, even if the table does not match `table_match_regex`.

### rule_match_mode [Enum]

For entries with the same exact `table_path`:

- Explicit `FIRST_MATCH`: use the first entry in list order.
- Explicit `ALL_MATCH`: apply all entries in list order. Each rule receives the schema and row produced by the previous rule.

Although the default is `FIRST_MATCH`, omitting this option rejects duplicate table paths during transform initialization. Set it explicitly if repeated paths are intentional.

### common options

Use `plugin_input` and `plugin_output` to connect this transform to other plugins. See [Transform Plugin](common-options/common-options.md).

## Multi-Table Configuration

The effective rule for each input table is:

1. Matching `table_transform` entry or entries.
2. Otherwise, the transform-level rule, if configured and selected by `table_match_regex`.
3. Otherwise, unchanged rows and schema.

A transform-level rule must be complete if provided. With only `table_transform`, unlisted tables pass through unchanged. See [Multi-Table Transform](transform-multi-table.md).

## Example

These complete batch configurations use two FakeSource tables, each with one `id` column. The Java code adds a constant `tag` column and keeps `id`. Console row order and generated IDs can vary; the expected table schemas and tags below do not depend on them.

| Table | Input schema | Transform-level rule only | With a per-table override | Per-table rules only |
| --- | --- | --- | --- | --- |
| `demo.orders` | `id INT` | `id INT, tag STRING` (`default`) | `id INT, tag STRING` (`orders`) | `id INT, tag STRING` (`orders`) |
| `demo.returns` | `id INT` | `id INT, tag STRING` (`default`) | `id INT, tag STRING` (`default`) | `id INT` (unchanged) |

### Transform-level rule only

Both `demo.orders` and `demo.returns` produce `id, tag`, with `tag = "default"`.

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

### Transform-level rule with a per-table override

`demo.orders` gets `tag = "orders"`; `demo.returns` gets `tag = "default"`. The orders rule replaces the shared rule; it is not applied after it.

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

### Per-table rules only

Only `demo.orders` gains `tag = "orders"`. `demo.returns` keeps its original single-column `id` schema, even though the default `table_match_regex` matches it.

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

### Groovy and Scala source shapes

To use Groovy, set `compile_language = "GROOVY"` and replace `source_code` in a rule with:

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

For Scala on Zeta, set `compile_language = "SCALA"` and use:

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

Both variants preserve `id` and add `tag` with the language name. For additional configurations, see the [DynamicCompile E2E fixtures](https://github.com/apache/seatunnel/tree/dev/seatunnel-e2e/seatunnel-transforms-v2-e2e/seatunnel-transforms-v2-e2e-part-2/src/test/resources/dynamic_compile/conf).

## Changelog

