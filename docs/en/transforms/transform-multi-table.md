---
sidebar_position: 2
---

# Multi-Table Transform in SeaTunnel

SeaTunnel’s transform feature supports multi-table transformations, which is especially useful when the upstream plugin outputs multiple tables. This allows you to complete all necessary transformation operations within a single transform configuration. Currently, many connectors in SeaTunnel support multi-table outputs, such as `JDBCSource` and `MySQL-CDC`. All transforms can be configured for multi-table transform as described below.

:::tip

Multi-table Transform has no limitations on Transform capabilities; any Transform configuration can be used in a multi-table Transform. The purpose of multi-table Transform is to handle multiple tables in the data stream individually and merge the Transform configurations of multiple tables into one Transform for easier management.

:::

## Properties

| Name                       | Type   | Required | Default | Description                                                                                                                                                                                                                                                     |
|----------------------------|--------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table_match_regex          | String | No       | .*      | A regular expression to match the tables that require transformation. By default, it matches all tables. Note that this table name refers to the actual upstream table name, not `plugin_output`.                                                               |
| table_transform            | List   | No       | -       | You can use a list in `table_transform` to specify rules for individual tables. If a transformation rule is configured for a specific table in `table_transform`, the outer rules will not apply to that table. The rules in `table_transform` take precedence. |
| table_transform.table_path | String | No       | -       | When configuring a transformation rule for a table in `table_transform`, you need to specify the table path using the `table_path` field. The table path should include `databaseName[.schemaName].tableName`.                                                  |

## Matching Logic

Suppose we read five tables from upstream: `test.abc`, `test.abcd`, `test.xyz`, `test.xyzxyz`, and `test.www`. They share the same structure, each having three fields: `id`, `name`, and `age`.

| id | name | age |

Now, let's say we want to copy the data from these five tables using the Copy transform with the following specific requirements:
- For tables `test.abc` and `test.abcd`, we need to copy the `name` field to a new field `name1`.
- For `test.xyz`, we want to copy the `name` field to `name2`.
- For `test.xyzxyz`, we want to copy the `name` field to `name3`.
- For `test.www`, no changes are needed.

We can configure this as follows:

```hocon
transform {
  Copy {
    plugin_input = "fake"  // Optional dataset name to read from
    plugin_output = "fake1" // Optional dataset name for output

    table_match_regex = "test.a.*" // 1. Matches tables needing transformation, here matching `test.abc` and `test.abcd`
    src_field = "name" // Source field
    dest_field = "name1" // Destination field

    table_transform = [{
      table_path = "test.xyz" // 2. Specifies the table name for transformation
      src_field = "name"  // Source field
      dest_field = "name2" // Destination field
    }, {
      table_path = "test.xyzxyz"
      src_field = "name"
      dest_field = "name3"
    }]
  }
}
```

### Explanation

1. With the regular expression and corresponding Copy transform options, we match tables `test.abc` and `test.abcd` and copy the `name` field to `name1`.
2. Using the `table_transform` configuration, we specify that for table `test.xyz`, the `name` field should be copied to `name2`.

This allows us to handle transformations for multiple tables within a single transform configuration.

For each table, the priority of configuration is: `table_transform` > `table_match_regex`. If no rules match a table, no transformation will be applied.

Below are the transform configurations for each table:

- **test.abc** and **test.abcd**

```hocon
transform {
  Copy {
    src_field = "name"
    dest_field = "name1"
  }
}
```

Output structure:

| id | name | age | name1 |

- **test.xyz**

```hocon
transform {
  Copy {
    src_field = "name"
    dest_field = "name2"
  }
}
```

Output structure:

| id | name | age | name2 |

- **test.xyzxyz**

```hocon
transform {
  Copy {
    src_field = "name"
    dest_field = "name3"
  }
}
```

Output structure:

| id | name | age | name3 |

- **test.www**

```hocon
transform {
  // No transformation needed
}
```

Output structure:

| id | name | age |

In this example, we used the Copy transform, but all transforms in SeaTunnel support multi-table transformations, and you can configure them similarly within the corresponding transform block.