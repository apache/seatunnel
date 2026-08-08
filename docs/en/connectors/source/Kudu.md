import ChangeLog from '../changelog/connector-kudu.md';

# Kudu Source Connector

`Source: Kudu`

Used to read data from Apache Kudu. The connector compiles table scans into server-side scan tokens so multiple readers can scan different key ranges of the same table in parallel, with predicates and column projection pushed down to Kudu.

The connector is at-least-once for batch jobs and does not read the Kudu change-log stream.

## Support Kudu Version

- 1.11.1/1.12.0/1.13.0/1.14.0/1.15.0

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

The tested Kudu version is 1.11.1.

## Data Type Mapping

|      kudu Data Type      | SeaTunnel Data Type |
|--------------------------|---------------------|
| BOOL                     | BOOLEAN             |
| INT8<br/>INT16<br/>INT32 | INT                 |
| INT64                    | BIGINT              |
| DECIMAL                  | DECIMAL             |
| FLOAT                    | FLOAT               |
| DOUBLE                   | DOUBLE              |
| STRING                   | STRING              |
| UNIXTIME_MICROS          | TIMESTAMP           |
| BINARY                   | BYTES               |

## Source Options

|                   Name                    | Type   | Required | Default                                        | Description                                                                                                                                                                                      |
|-------------------------------------------|--------|----------|------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| kudu_masters                              | String | Yes      | -                                              | Comma-separated list of Kudu master addresses, for example `192.168.88.110:7051`. The connector uses this address to derive the client worker count and operation timeouts.                                              |
| table_name                                | String | Yes (unless `table_list` is set) | -                                              | The name of the Kudu table to read. This option is mutually exclusive with `table_list`. When `use_regex = true`, the value is treated as a Java regular expression and may match multiple tables.                              |
| client_worker_count                       | Int    | No       | 2 * Runtime.getRuntime().availableProcessors() | Number of Kudu client workers. Default value is twice the number of CPU cores available to the JVM.                                                                                                                       |
| client_default_operation_timeout_ms       | Long   | No       | 30000                                          | Default Kudu operation timeout in milliseconds.                                                                                                                                                                  |
| client_default_admin_operation_timeout_ms | Long   | No       | 30000                                          | Default Kudu admin operation timeout in milliseconds.                                                                                                                                                                   |
| enable_kerberos                           | Bool   | No       | false                                          | Whether to enable Kerberos authentication for the Kudu client. Set to `true` together with `kerberos_principal` and `kerberos_keytab`.                                                                                                                                                                       |
| kerberos_principal                        | String | Conditional (when `enable_kerberos = true`) | -                                              | Kerberos principal used by the Kudu client. The keytab must be available on every worker node.                                                                                       |
| kerberos_keytab                           | String | Conditional (when `enable_kerberos = true`) | -                                              | Kerberos keytab path used by the Kudu client. The file must be available on every worker node.                                                                                       |
| kerberos_krb5conf                         | String | No       | -                                              | Kerberos `krb5.conf` path. Required on every Zeta node that runs the connector.                                                                                                                             |
| scan_token_query_timeout                  | Long   | No       | 30000                                          | Timeout for connecting to scan tokens in milliseconds. If not set, it defaults to `client_default_operation_timeout_ms`.                                                                                                      |
| scan_token_batch_size_bytes               | Int    | No       | 1024 * 1024                                    | Maximum bytes a single Kudu scan token returns in one batch. Default is 1 MiB.                                                                                                                 |
| use_regex                                 | Bool   | No       | false                                          | When `true`, treat `table_name` as a Java regular expression and match multiple tables. When `false` (the default), `table_name` is treated as an exact table name with no regex matching. This can be set either at the top level or inside each `table_list` entry. |
| filter                                    | String | No       | -                                              | Kudu scan filter expression that is pushed down to the server, for example `id > 100 AND id < 200`.                                                                                                                                      |
| schema                                    | Map    | No       | -                                              | SeaTunnel schema describing the columns to read. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).                                                                                                                                                                                |
| table_list                                | Array  | No       | -                                              | List of tables to read. Use this option instead of `table_name`, for example ```table_list = [{ table_name = "kudu_source_table_1"},{ table_name = "kudu_source_table_2"}] ```. Each entry may set `use_regex = true` to enable regex matching for that `table_name`. |
| common-options                            |        | No       | -                                              | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                               |

## Option Notes

- Configure exactly one of `table_name` and `table_list`.
- `filter` is pushed down to Kudu scans and can use Kudu predicate expressions such as `id >= 1 AND id <= 2`.
- `use_regex = true` treats `table_name` as a Java regular expression. This can be used either at the top level or inside each `table_list` item.
- When `enable_kerberos = true`, both `kerberos_principal` and `kerberos_keytab` are required.

## Task Example

### Simple

> The following example is for a Kudu table named "kudu_source_table", The goal is to print the data from this table on the console and write kudu table "kudu_sink_table"

```hocon
# Defining the runtime environment
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  kudu {
    kudu_masters = "kudu-master:7051"
    table_name = "kudu_source_table"
    plugin_output = "kudu"
    enable_kerberos = true
    kerberos_principal = "xx@xx.COM"
    kerberos_keytab = "xx.keytab"
  }
}

transform {
}

sink {
  console {
    plugin_input = "kudu"
  }

  kudu {
    plugin_input = "kudu"
    kudu_masters = "kudu-master:7051"
    table_name = "kudu_sink_table"
    enable_kerberos = true
    kerberos_principal = "xx@xx.COM"
    kerberos_keytab = "xx.keytab"
  }
}
```

### Multiple Table

```hocon
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  kudu{
   kudu_masters = "kudu-master:7051"
   table_list = [
   {
    table_name = "kudu_source_table_1"
   },{
    table_name = "kudu_source_table_2"
   }
   ]
   plugin_output = "kudu"
}
}

transform {
}

sink {
  Assert {
    rules {
      table-names = ["kudu_source_table_1", "kudu_source_table_2"]
    }
  }
}
```

### Table Matching With Regex

The Kudu Source supports using regular expressions on `table_name` to match multiple tables (including whole-database style synchronization, since Kudu tables are in a single logical database).

#### Exact Table Name

Use `table_name` to specify a single Kudu table with an exact name:

```hocon
source {
  kudu {
    kudu_masters = "kudu-master:7051"
    table_name = "kudu_source_table_1"
  }
}
```

#### Regex Matching

Use `table_name` as a regex pattern and enable `use_regex` to read multiple tables with one configuration:

```hocon
source {
  kudu {
    kudu_masters = "kudu-master:7051"
    # Match tables like kudu_source_table_1, kudu_source_table_2, etc.
    table_name = "kudu_source_table_\\d+"
    use_regex = true
  }
}
```

You can also combine regex entries in `table_list`:

```hocon
source {
  kudu {
    kudu_masters = "kudu-master:7051"
    table_list = [
      {
        table_name = "kudu_source_table_1"
      },
      {
        table_name = "kudu_source_table_2"
      },
      {
        # Regex matching - any table whose name starts with prefix_ and ends with digits
        table_name = "prefix_\\d+"
        use_regex = true
      }
    ]
  }
}
```

#### Whole-Database Matching

You can also synchronize all tables in the current Kudu cluster (or all business tables in the current instance, if there are no system tables) by using a catch-all regex:

```hocon
source {
  kudu {
    kudu_masters = "kudu-master:7051"
    # Match all tables in the current Kudu cluster
    table_name = ".*"
    use_regex = true
  }
}
```

## Changelog

<ChangeLog />
