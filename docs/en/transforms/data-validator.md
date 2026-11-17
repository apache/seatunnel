# DataValidator

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark

## Description

DataValidator checks field values against configurable validation rules. It is a row-level quality gate: valid rows are passed through unchanged, and the input schema is never modified. When a row fails any rule, the transform applies the configured error handling policy: fail the job (`FAIL`, the default), drop the row (`SKIP`), or convert the row into a structured error record and route it to a dedicated error table (`ROUTE_TO_TABLE`).

All configured fields of every row are validated (there is no fail-fast): a row is invalid when at least one rule fails, and all failures of the row are collected into the error messages.

## Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| field_rules | Array&lt;Map&gt; | Conditional | - | Validation rules grouped by field name. Required by the transform-level rule and by every `table_transform` entry. |
| row_error_handle_way | Enum | No | `FAIL` | What to do with an invalid row: `FAIL`, `SKIP`, or `ROUTE_TO_TABLE`. |
| row_error_handle_way.error_table | String | Conditional | - | Target table for routed invalid rows when `row_error_handle_way` is `ROUTE_TO_TABLE`. |
| table_match_regex | String | No | `.*` | Match upstream table paths to which the transform-level rule applies. |
| table_transform | Array&lt;Map&gt; | No | `[]` | Complete rules for individual tables, selected by an exact `table_path`. |
| rule_match_mode | Enum | No | `FIRST_MATCH` | Select the first matching per-table rule, or apply all matching rules in order. See the duplicate-path behavior below. |
| common options | - | No | - | Common transform options such as `plugin_input` and `plugin_output`. |

### field_rules [Array&lt;Map&gt;]

The validation rules. Without a nonempty `table_transform` list, `field_rules` is required at the transform level; an empty `table_transform = []` does not remove this requirement.

Each entry describes the rules of one field and supports two equivalent shapes:

- **Nested**: one entry per field, with a `rules` list inside:

```hocon
field_rules = [
  {
    field_name = "age"
    rules = [
      { rule_type = "NOT_NULL" },
      { rule_type = "RANGE", min_value = "0", max_value = "150" }
    ]
  }
]
```

- **Flat**: one entry per rule, with the rule parameters directly in the entry. Multiple flat entries for the same field are merged into one field rule:

```hocon
field_rules = [
  { field_name = "age", rule_type = "NOT_NULL" },
  { field_name = "age", rule_type = "RANGE", min_value = "0", max_value = "150" }
]
```

Every entry must contain a nonblank `field_name` and either a `rule_type` or a non-empty `rules` list; otherwise job submission fails with a validation error. Rules for a field that is not present in the input schema are skipped with a warning, and unknown rule types are ignored with a warning.

The supported rule types:

| rule_type | Parameters | Description |
|-----------|------------|-------------|
| `NOT_NULL` | custom_message | Fails when the value is `null`. |
| `RANGE` | min_value, max_value, min_inclusive, max_inclusive, custom_message | Fails when the value is outside the range. Both bounds are inclusive by default. A `null` value passes this rule — combine it with `NOT_NULL` to reject nulls. |
| `LENGTH` | min_length, max_length, exact_length, custom_message | Checks the length of strings, arrays and collections. `exact_length` is checked first, then `min_length` / `max_length`. A `null` value passes this rule. |
| `REGEX` | pattern (required), case_sensitive, custom_message | Matches the value against a Java regular expression. Matching is case-sensitive by default. |
| `UDF` | function_name (required), custom_message | Runs a built-in or custom validation function. Built-in: `EMAIL` (practical email validation based on OWASP recommendations). |

Notes on the rule parameters:

- **min_value / max_value**: compared as `BigDecimal` when both sides are numeric, so the field type does not have to match the bound type; a value that cannot be parsed as a number is compared by its string representation.
- **min_inclusive / max_inclusive**: default `true` (closed range). Set to `false` for an open bound.
- **min_length / max_length / exact_length**: integers. String length is counted in Java UTF-16 code units; array length is the number of elements (the number of bytes for binary arrays); collection length is its size.
- **case_sensitive**: default `true`.
- **custom_message**: replaces the default error message of the rule. It appears both in the exception message of `FAIL` mode and in the `validation_errors` column of the error table.
- Rule parameter values may be written as numbers or as numeric strings (for example `min_length = 2` and `min_length = "2"` are equivalent).

### row_error_handle_way [Enum]

What happens to a row that fails at least one rule:

- `FAIL` (default): the transform throws a validation error and the job fails. The exception message joins the error messages of all failed rules of the row.
- `SKIP`: invalid rows are dropped; valid rows continue downstream.
- `ROUTE_TO_TABLE`: invalid rows are converted into error records and routed to the error table configured by `row_error_handle_way.error_table`. If no error table is configured (or the value is blank), invalid rows are dropped with a warning.

:::caution
`ROUTE_TO_TABLE` requires a sink that supports multiple tables: the transform produces the validated table plus the error table, and the sink has to write both.
:::

### row_error_handle_way.error_table [String]

The table that receives the routed invalid rows. The value is either:

- a plain table name without a dot — the error table is placed in the same database/schema as the input table;
- a path containing a dot (for example `error_db.error_data` or `db.error_schema.error_data`) — used as the table path as-is.

Each routed row is an error record with the following fixed schema:

| Column | Type | Description |
|--------|------|-------------|
| source_table_id | String | Full identifier of the table the row came from (catalog.database.schema.table, with the parts that exist). |
| source_table_path | String | Table path of the source table. |
| original_data | String | JSON object of the complete original row. |
| validation_errors | String | JSON array of `{field_name, error_message}` objects, one per failed rule. |
| create_time | LocalDateTime | When the error record was produced. |

### table_match_regex [String]

A Java regular expression matched against the whole upstream table path, such as `test.abc` or `database.schema.table`. The default `.*` matches all table paths. This is the actual table path, not the dataset name in `plugin_input` or `plugin_output`.

This option selects the tables to which the transform-level rule applies. It does not restrict explicit `table_transform` entries.

### table_transform [Array&lt;Map&gt;]

A list of per-table rules; the default is an empty list. Each entry must have a nonblank `table_path` (an exact path, not a regular expression) in the form `databaseName[.schemaName].tableName`, plus a complete single-table DataValidator rule for that table: `field_rules` and an optional error handling policy.

An entry declares its own `field_rules` and does not inherit the transform-level `field_rules`. A matching entry takes precedence even when its table does not match `table_match_regex`. The error handling policy is the exception: see `Multi-Table Configuration` below.

### rule_match_mode [Enum]

Controls multiple entries with the same exact `table_path`:

- Explicit `FIRST_MATCH`: use the first matching entry in list order.
- Explicit `ALL_MATCH`: apply all matching entries in list order; the output of an earlier matching entry becomes the input of the next matching entry for the same table.

Although the option's default is `FIRST_MATCH`, omitting the option rejects duplicate `table_path` entries during transform initialization. Set it explicitly if repeated paths are intentional. With unique paths, the default is sufficient.

### common options

Use `plugin_input` and `plugin_output` to connect this transform to upstream and downstream plugins. See [Transform Plugin](common-options/common-options.md) for common parameters.

## Multi-Table Configuration

For each input table, the effective rule is selected in this order:

1. Use the matching `table_transform` entry or entries, according to `rule_match_mode`.
2. Otherwise, apply the transform-level rule if one is configured (see `field_rules`) and the table path matches `table_match_regex`.
3. Otherwise, pass the table's rows through unchanged.

In a `table_transform`-only configuration, unlisted tables pass through unchanged. The presence of `field_rules` marks the transform-level rule as configured: without it, the other transform-level options have no effect on the field rules.

DataValidator has one option group that behaves differently: `row_error_handle_way` and `row_error_handle_way.error_table` are error handling **policies**. They are configured once at the transform level and also apply to tables selected by a `table_transform` entry, unless the entry sets its own value. This makes a single error table usable for every validated table.

The following examples cover all three configuration shapes. See [Multi-Table Transform](transform-multi-table.md) for the shared matching rules.

## Examples

These complete batch configurations use FakeSource and Console. Install those connectors in your SeaTunnel distribution before running a saved configuration with `bin/seatunnel.sh --config <file> -e local`. The rows are fixed by the `rows` option of FakeSource, so the expected outputs are deterministic.

### Transform-level rules only, with ROUTE_TO_TABLE

The input has six rows; three of them violate the rules (a `null` name, an age of 200, and a malformed email). With `ROUTE_TO_TABLE`, the valid rows keep flowing to the validated output while each invalid row is converted into an error record and routed to `error_data`:

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    schema {
      fields {
        id = "int"
        name = "string"
        age = "int"
        email = "string"
      }
    }
    rows = [
      { fields = [1, "John Doe", 25, "john@example.com"], kind = INSERT }
      { fields = [2, "Jane Smith", 30, "jane@example.com"], kind = INSERT }
      { fields = [3, "Charlie Wilson", 32, "charlie@example.com"], kind = INSERT }
      { fields = [4, null, 30, "invalid@example.com"], kind = INSERT }
      { fields = [5, "Bob Johnson", 200, "bob@example.com"], kind = INSERT }
      { fields = [6, "Alice Brown", 28, "invalid-email"], kind = INSERT }
    ]
  }
}

transform {
  DataValidator {
    plugin_input = "fake"
    plugin_output = "validated"
    row_error_handle_way = "ROUTE_TO_TABLE"
    row_error_handle_way.error_table = "error_data"
    field_rules = [
      {
        field_name = "name"
        rule_type = "NOT_NULL"
        custom_message = "name must not be null"
      },
      {
        field_name = "age"
        rule_type = "RANGE"
        min_value = "0"
        max_value = "150"
      },
      {
        field_name = "email"
        rule_type = "REGEX"
        pattern = "^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}$"
        custom_message = "invalid email format"
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "validated"
  }
}
```

The `validated` output keeps rows 1–3 unchanged. The `error_data` table receives three error records; for example the record for row 5 contains:

| source_table_id | source_table_path | original_data | validation_errors | create_time |
|----------------|-------------------|--------------|------------------|-------------|
| ... | fake | {"id":5,"name":"Bob Johnson","age":200,"email":"bob@example.com"} | [{"field_name":"age","error_message":"Value 200 exceeds maximum 150"}] | ... |

and the record for row 6 contains `[{"field_name":"email","error_message":"invalid email format"}]` in `validation_errors`, because the `custom_message` of the rule replaces the default message.

:::tip
With `row_error_handle_way = "FAIL"` (the default) the same configuration fails the job on the first invalid row; with `SKIP` the three invalid rows are silently dropped and only rows 1–3 reach the sink.
:::

### Transform-level rule with per-table rules

Each input table contains two valid rows. `test.abc` is validated by the transform-level `field_rules`, while `test.xyz` uses its own `table_transform` entry. The error handling policy (`FAIL`) is set once at the transform level and applies to both tables.

| Table | Effective field rules | Output rows |
|-------|----------------------|-------------|
| `test.abc` | Transform level: `name` NOT_NULL, `age` RANGE 0–150. | 2 |
| `test.xyz` | Entry: `name` NOT_NULL, `email` REGEX. | 2 |

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        schema {
          table = "test.abc"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" },
            { name = "email", type = "string" }
          ]
        }
        rows = [
          { fields = [1, "John Doe", 25, "john@example.com"], kind = INSERT }
          { fields = [2, "Jane Smith", 30, "jane@example.com"], kind = INSERT }
        ]
      },
      {
        schema {
          table = "test.xyz"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" },
            { name = "email", type = "string" }
          ]
        }
        rows = [
          { fields = [1, "Bob Johnson", 35, "bob@example.com"], kind = INSERT }
          { fields = [2, "Alice Brown", 28, "alice@example.com"], kind = INSERT }
        ]
      }
    ]
  }
}

transform {
  DataValidator {
    plugin_input = "fake"
    plugin_output = "validated"
    row_error_handle_way = "FAIL"
    // Applies to every table that is not covered by a table_transform entry.
    field_rules = [
      {
        field_name = "name"
        rule_type = "NOT_NULL"
      },
      {
        field_name = "age"
        rule_type = "RANGE"
        min_value = "0"
        max_value = "150"
      }
    ]

    table_transform = [
      {
        table_path = "test.xyz"
        field_rules = [
          {
            field_name = "name"
            rule_type = "NOT_NULL"
          },
          {
            field_name = "email"
            rule_type = "REGEX"
            pattern = "^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}$"
          }
        ]
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "validated"
  }
}
```

### Per-table rules only

There is no transform-level `field_rules`: every table is validated by its own `table_transform` entry, and any other table would pass through unchanged. The error handling policy is still configured at the transform level and applies to every validated table.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        schema {
          table = "test.abc"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" },
            { name = "email", type = "string" }
          ]
        }
        rows = [
          { fields = [1, "John Doe", 25, "john@example.com"], kind = INSERT }
          { fields = [2, "Jane Smith", 30, "jane@example.com"], kind = INSERT }
        ]
      },
      {
        schema {
          table = "test.xyz"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" },
            { name = "email", type = "string" }
          ]
        }
        rows = [
          { fields = [1, "Bob Johnson", 35, "bob@example.com"], kind = INSERT }
          { fields = [2, "Alice Brown", 28, "alice@example.com"], kind = INSERT }
        ]
      }
    ]
  }
}

transform {
  DataValidator {
    plugin_input = "fake"
    plugin_output = "validated"
    row_error_handle_way = "FAIL"
    table_transform = [
      {
        table_path = "test.abc"
        field_rules = [
          {
            field_name = "name"
            rule_type = "NOT_NULL"
          },
          {
            field_name = "age"
            rule_type = "RANGE"
            min_value = "0"
            max_value = "150"
          }
        ]
      },
      {
        table_path = "test.xyz"
        field_rules = [
          {
            field_name = "name"
            rule_type = "NOT_NULL"
          },
          {
            field_name = "email"
            rule_type = "REGEX"
            pattern = "^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}$"
          }
        ]
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "validated"
  }
}
```

Both tables are valid in this input, so all four rows pass validation. An invalid row in `test.abc` would fail the job, because the transform-level `row_error_handle_way = "FAIL"` policy applies even without a transform-level `field_rules`; a table without an entry would pass through without validation.

## Developing a Custom UDF Rule

Besides the built-in `EMAIL` function, you can add your own validation function:

1. Implement the `DataValidatorUDF` interface (`org.apache.seatunnel.transform.validator.udf.DataValidatorUDF`), returning a unique `functionName()` and the `validate()` logic:

```java
@AutoService(DataValidatorUDF.class)
public class PhoneValidator implements DataValidatorUDF {

    @Override
    public String functionName() {
        return "PHONE";
    }

    @Override
    public ValidationResult validate(
            Object value, SeaTunnelDataType<?> dataType, ValidationContext context) {
        if (value == null) {
            return ValidationResult.success();
        }
        return value.toString().trim().matches("^\\+?[1-9]\\d{1,14}$")
                ? ValidationResult.success()
                : ValidationResult.failure("Invalid phone number format: " + value);
    }
}
```

2. Annotate the class with `@AutoService(DataValidatorUDF.class)` so it is discovered by the Java `ServiceLoader` mechanism.
3. Package the compiled class into a JAR and put it on the SeaTunnel classpath; the function is then available by its `function_name`.
