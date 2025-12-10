# Pivot

> Pivot (Row-to-Column) transform plugin

## Description

The Pivot transform converts multiple rows into columns by pivoting on a specified column. This is useful for transforming normalized data into a denormalized format, where values from one column become new column names.

This transform supports **batch processing with checkpoint capability**, meaning it can accumulate data and survive failures through SeaTunnel's checkpoint mechanism.

## Options

|       name        |  type  | required |  default value  |
|-------------------|--------|----------|-----------------|
| group_by_keys     | array  | yes      |                 |
| pivot_column      | string | yes      |                 |
| value_column      | string | yes      |                 |
| pivot_values      | array  | yes      |                 |
| default_value     | string | no       | null            |
| max_buffer_size   | int    | no       | 10000           |
| group_timeout_ms  | long   | no       | -1              |

### group_by_keys [array]

The columns used to group rows together. Rows with the same values in these columns will be combined into a single output row.

### pivot_column [string]

The column whose values will become new column names in the output. Each unique value in this column creates a new column.

### value_column [string]

The column whose values will populate the new pivoted columns.

### pivot_values [array]

Pre-defined list of pivot values. Only these values from `pivot_column` will create new columns. This is required to define the output schema.

### default_value [string]

The default value to use when a pivot value is missing for a group. Defaults to null.

### max_buffer_size [int]

Maximum number of groups to buffer before forcing a flush. This helps control memory usage for streaming scenarios. Set to -1 for unlimited buffering (flush only on checkpoint). Default is 10000.

### group_timeout_ms [long]

Timeout in milliseconds for a group. If a group hasn't received new data within this timeout, it will be flushed. Set to -1 to disable timeout-based flushing. Default is -1.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details.

## How It Works

The Pivot transform:

1. **Collects** incoming rows and groups them by `group_by_keys`
2. **Extracts** the pivot key from `pivot_column` and the value from `value_column`
3. **Stores** the value in the corresponding pivot column for each group
4. **Flushes** the accumulated groups during checkpoint or when buffer is full
5. **Outputs** one row per group with the original group-by columns plus the new pivot columns

## Example

### Basic Example

The data read from source is a table like this:

| id | type | value |
|----|------|-------|
| 1  | A    | 100   |
| 1  | B    | 200   |
| 2  | A    | 150   |
| 2  | C    | 300   |

We want to pivot the `type` column, using `value` column values, grouped by `id`:

```hocon
transform {
  Pivot {
    plugin_input = "fake"
    plugin_output = "pivoted"
    group_by_keys = ["id"]
    pivot_column = "type"
    value_column = "value"
    pivot_values = ["A", "B", "C"]
  }
}
```

Then the data in result table `pivoted` will be:

| id | A   | B    | C    |
|----|-----|------|------|
| 1  | 100 | 200  | null |
| 2  | 150 | null | 300  |

### Multiple Group-By Keys Example

For data with multiple grouping columns:

| store_id | date       | metric  | value |
|----------|------------|---------|-------|
| 1        | 2024-01-01 | sales   | 1000  |
| 1        | 2024-01-01 | returns | 50    |
| 1        | 2024-01-02 | sales   | 1200  |
| 2        | 2024-01-01 | sales   | 800   |

Configuration:

```hocon
transform {
  Pivot {
    plugin_input = "source"
    plugin_output = "pivoted"
    group_by_keys = ["store_id", "date"]
    pivot_column = "metric"
    value_column = "value"
    pivot_values = ["sales", "returns", "profit"]
  }
}
```

Result:

| store_id | date       | sales | returns | profit |
|----------|------------|-------|---------|--------|
| 1        | 2024-01-01 | 1000  | 50      | null   |
| 1        | 2024-01-02 | 1200  | null    | null   |
| 2        | 2024-01-01 | 800   | null    | null   |

### Complete Job Configuration

```hocon
env {
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        type = "string"
        value = "int"
      }
    }
    rows = [
      { fields = [1, "A", 100], kind = INSERT }
      { fields = [1, "B", 200], kind = INSERT }
      { fields = [2, "A", 150], kind = INSERT }
      { fields = [2, "C", 300], kind = INSERT }
    ]
  }
}

transform {
  Pivot {
    plugin_input = "fake"
    plugin_output = "pivoted"
    group_by_keys = ["id"]
    pivot_column = "type"
    value_column = "value"
    pivot_values = ["A", "B", "C"]
  }
}

sink {
  Console {
    plugin_input = "pivoted"
  }
}
```

## Checkpoint Support

The Pivot transform implements stateful processing with checkpoint support:

- **State Persistence**: Buffered groups are saved during checkpoints
- **Fault Tolerance**: After failure recovery, the transform restores its buffer from the last checkpoint
- **At-Least-Once Semantics**: Data is flushed at each checkpoint to ensure no data loss

This is particularly important for streaming jobs where the transform accumulates data over time.

## Changelog

### new version

- Add Pivot Transform Connector with checkpoint support
