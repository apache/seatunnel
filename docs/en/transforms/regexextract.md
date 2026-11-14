# RegexExtract

> RegexExtract transform plugin

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Description

The RegexExtract transform uses a Java regular expression to extract data from one input field and writes the value of each capture group to a new output field. It is typically used to split semi-structured strings such as email addresses, log lines or file paths into separate columns during ingestion. When the pattern does not match, or the input value is null, each output field receives its configured default value instead (or null when no default is configured). RegexExtract supports multi-table configuration, see [Multi-Table Configuration](#multi-table-configuration).

## Key Features

- Extract one capture group per output field from a single source field
- The pattern is searched in the value (`Matcher#find`), so it does not have to match the whole value
- `default_values` provide a fallback for null values and non-matching values
- The produced columns are always `STRING` columns and are appended after the input columns; the original field is kept
- Works on multiple tables in one transform, see [Multi-Table Transform](transform-multi-table.md)

## Options

| Name              | Type               | Required | Default       | Description                                                                                          |
|-------------------|--------------------|----------|---------------|------------------------------------------------------------------------------------------------------|
| source_field      | String             | yes      | -             | Input field to extract from. It must exist in the input table.                                       |
| regex_pattern     | String             | yes      | -             | Regular expression with capture groups. The number of capture groups must equal the number of `output_fields`. |
| output_fields     | Array&lt;String&gt; | yes      | -             | Names of the output fields, in capture group order. The list must not be empty.                      |
| default_values    | Array&lt;String&gt; | no       | -             | Default value per output field, used when the pattern does not match or the source value is null. When set, its length must equal the length of `output_fields`. |
| table_match_regex | String             | no       | `.*`          | Regex of the table paths that the transform level rule applies to.                                   |
| table_transform   | Array&lt;Map&gt;    | no       | -             | Per-table rules, see [Multi-Table Transform](transform-multi-table.md).                              |
| rule_match_mode   | String             | no       | `FIRST_MATCH` (duplicate `table_path` entries are rejected unless set explicitly) | How multiple `table_transform` entries with the same `table_path` are evaluated.                     |
| common options    | -                  | no       | -             | Transform plugin common parameters, see [Transform Plugin](common-options/common-options.md).       |

:::info

`source_field`, `regex_pattern` and `output_fields` are required in every rule. In a multi-table configuration, the transform level rule can be omitted, but each `table_transform` entry must still contain all three options. Tables without a matching entry are passed through unchanged.

:::

### source_field [String]

The name of the input field to extract from. The value is converted with `toString()` before matching, so fields of other types are accepted as well.

The field must exist in the input table. Otherwise the job fails with error code `TRANSFORM_COMMON-01`: `The input field 'email' of 'RegexExtract' transform not found in upstream schema`.

### regex_pattern [String]

The Java regular expression, compiled with `java.util.regex.Pattern`. It must contain one capture group per entry of `output_fields`: if the number of capture groups differs from the number of output fields, the job fails when the transform is created.

The pattern is applied with `Matcher#find`, which means it is searched anywhere in the value and does not need to match the whole value; the first match is used. A capture group that does not take part in the match produces `null` for its output field.

Because the pattern is written inside a HOCON string, backslashes must be escaped, for example `regex_pattern = "([^@]+)@([^.]+)\\.(.+)"`.

### output_fields [Array&lt;String&gt;]

The names of the fields produced by the transform, in capture group order: the first output field receives capture group 1, the second receives capture group 2, and so on. The list must not be empty.

New output columns are appended after the existing input columns; the input field is not removed. Every column appended this way is a nullable `STRING` column with length 200. If an output field name already exists in the input table, that column is reused and overwritten instead of appended: the extracted value replaces its value, but the column keeps its original schema metadata — in particular, a reused column does not automatically become nullable or change its length. The only metadata that can change is the data type: if it differs from `STRING`, the column is copied with the `STRING` data type while its other metadata is retained.

### default_values [Array&lt;String&gt;]

The fallback value for each output field, in the same order as `output_fields`. The defaults are used in two cases:

- the source value is `null`;
- the source value does not match `regex_pattern`.

When `default_values` is set, its length must equal the length of `output_fields`, otherwise the configuration is rejected. When it is not set, output fields receive `null` in both cases above.

### table_match_regex [String]

A regular expression matched against the upstream table path. The transform level rule (`source_field`, `regex_pattern`, `output_fields` and `default_values` outside of `table_transform`) is applied only to the tables that match. The default `.*` matches every table. Tables that match neither this regex nor a `table_transform` entry are passed through unchanged.

Note that this refers to the real upstream table name, not to `plugin_output`.

### table_transform [Array&lt;Map&gt;]

A list of per-table rules. Each entry must contain `table_path` in the form `databaseName[.schemaName].tableName` (matched exactly) and a complete RegexExtract rule for that table, which means `source_field`, `regex_pattern` and `output_fields`, plus `default_values` when needed.

When a table has an entry in `table_transform`, the transform level rule is not applied to it. See [Multi-Table Configuration](#multi-table-configuration).

### rule_match_mode [String]

Controls what happens when several `table_transform` entries have the same `table_path`. Supported values are `FIRST_MATCH` (apply only the first matching entry) and `ALL_MATCH` (apply all matching entries in order, each one receiving the output of the previous one).

If `rule_match_mode` is not set, duplicate `table_path` entries are rejected. Set it explicitly to `FIRST_MATCH` or `ALL_MATCH` when several entries use the same `table_path`.

### common options

Transform plugin common parameters, please refer to [Transform Plugin](common-options/common-options.md) for details.

## Multi-Table Configuration

When the upstream source outputs several tables (for example JDBC or MySQL-CDC), one RegexExtract transform can handle all of them.

- Entries in `table_transform` take precedence over the transform level rule. A table that has an entry is never touched by the transform level rule.
- Every entry must be a complete single-table rule (`table_path` plus `source_field`, `regex_pattern` and `output_fields`).
- The transform level rule applies only to tables matching `table_match_regex`.
- When only `table_transform` is configured, the tables without an entry stay unchanged.

The configuration below uses a transform level rule for `test.a.*` tables, a per-table rule for `test.xyz`, and leaves `test.www` unchanged:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        row.num = 2
        schema = {
          table = "test.abc"
          fields { id = int, email = string }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.abcd"
          fields { id = int, email = string }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.xyz"
          fields { id = int, email = string }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.www"
          fields { id = int, email = string }
        }
      }
    ]
  }
}

transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "fake1"

    table_match_regex = "test.a.*"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = [username, domain, tld]

    table_transform = [
      {
        table_path = "test.xyz"
        source_field = "email"
        regex_pattern = "([^@]+)@(.+)"
        output_fields = [username, host]
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

Result schema of each table:

| Table      | Rule applied                              | Output fields                    |
|------------|-------------------------------------------|----------------------------------|
| test.abc   | transform level rule (matches `test.a.*`) | id, email, username, domain, tld |
| test.abcd  | transform level rule (matches `test.a.*`) | id, email, username, domain, tld |
| test.xyz   | `table_transform` entry                   | id, email, username, host        |
| test.www   | none                                      | id, email                        |

## Example

### Single table

The data read from source is a table like this:

| id | email             | log_entry                                            |
|----|-------------------|------------------------------------------------------|
| 1  | user1@example.com | 2023-12-01 10:30:45 INFO User login successful       |
| 2  | admin@test.org    | 2023-12-01 11:15:22 ERROR Database connection failed |
| 3  | guest@domain.net  | 2023-12-01 12:00:00 WARN Memory usage high           |

We want to extract username, domain, and top-level domain from the `email` field:

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
        email = "string"
        log_entry = "string"
      }
    }
    rows = [
      {
          kind = INSERT,
          fields = [1, "user1@example.com", "2023-12-01 10:30:45 INFO User login successful"]
      },
      {
        kind = INSERT,
        fields = [2, "admin@test.org", "2023-12-01 11:15:22 ERROR Database connection failed"]
      },
      {
        kind = INSERT,
        fields = [3, "guest@domain.net", "2023-12-01 12:00:00 WARN Memory usage high"]
      }
    ]
  }
}

transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "regex_result"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = ["username", "domain", "tld"]
    default_values = ["unknown", "unknown", "unknown"]
  }
}

sink {
  Console {
    plugin_input = "regex_result"
  }
}
```

Then the data in result table `regex_result` will be:

| id | email             | log_entry                                            | username | domain  | tld |
|----|-------------------|------------------------------------------------------|----------|---------|-----|
| 1  | user1@example.com | 2023-12-01 10:30:45 INFO User login successful       | user1    | example | com |
| 2  | admin@test.org    | 2023-12-01 11:15:22 ERROR Database connection failed | admin    | test    | org |
| 3  | guest@domain.net  | 2023-12-01 12:00:00 WARN Memory usage high           | guest    | domain  | net |

A row whose `email` does not match the pattern, or is `null`, would get `unknown` for all three output fields because `default_values` is configured here. Without `default_values`, the three output fields would be `null` for such a row.

### Multiple tables

The `env`, `source` and `sink` blocks are the same as in [Multi-Table Configuration](#multi-table-configuration). Only the `transform` block changes. Assume the source outputs `test.abc`, `test.abcd`, `test.xyz` and `test.www`, each with the fields `id` and `email`.

**1. Transform level rule only.** The rule applies to every table because `table_match_regex` defaults to `.*`.

```hocon
transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "fake1"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = [username, domain, tld]
  }
}
```

**2. Transform level rule plus per-table rules.** See the complete job in [Multi-Table Configuration](#multi-table-configuration).

```hocon
transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "fake1"
    table_match_regex = "test.a.*"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = [username, domain, tld]
    table_transform = [
      {
        table_path = "test.xyz"
        source_field = "email"
        regex_pattern = "([^@]+)@(.+)"
        output_fields = [username, host]
      }
    ]
  }
}
```

**3. Per-table rules only.** The transform level rule can be omitted; tables without an entry are passed through unchanged.

```hocon
transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "fake1"
    table_transform = [
      {
        table_path = "test.abc"
        source_field = "email"
        regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
        output_fields = [username, domain, tld]
      },
      {
        table_path = "test.abcd"
        source_field = "email"
        regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
        output_fields = [username, domain, tld]
      },
      {
        table_path = "test.xyz"
        source_field = "email"
        regex_pattern = "([^@]+)@(.+)"
        output_fields = [username, host]
      }
    ]
  }
}
```

`test.www` has no matching entry and is passed through unchanged.

## Changelog

### new version

- Add RegexExtract Transform Plugin
