# StarRocks

> StarRocks source connector

## Description

Read external data source data through StarRocks.
The internal implementation of StarRocks source connector is obtains the query plan from the frontend (FE),
delivers the query plan as a parameter to BE nodes, and then obtains data results from BE nodes.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                     |  type  | required |   default value   |
|--------------------------|--------|----------|-------------------|
| nodeUrls                 | list   | yes      | -                 |
| username                 | string | yes      | -                 |
| password                 | string | yes      | -                 |
| database                 | string | yes      | -                 |
| table                    | string | yes      | -                 |
| scan_filter              | string | no       | -                 |
| schema                   | config | yes      | -                 |
| request_tablet_size      | int    | no       | Integer.MAX_VALUE |
| scan_connect_timeout_ms  | int    | no       | 30000             |
| scan_query_timeout_sec   | int    | no       | 3600              |
| scan_keep_alive_min      | int    | no       | 10                |
| scan_batch_rows          | int    | no       | 1024              |
| scan_mem_limit           | long   | no       | 2147483648        |
| max_retries              | int    | no       | 3                 |
| scan.params.*            | string | no       | -                 |

### nodeUrls [list]

`StarRocks` cluster address, the format is `["fe_ip:fe_http_port", ...]`

### username [string]

`StarRocks` user username

### password [string]

`StarRocks` user password

### database [string]

The name of StarRocks database

### table [string]

The name of StarRocks table

### scan_filter [string]

Filter expression of the query, which is transparently transmitted to StarRocks. StarRocks uses this expression to complete source-side data filtering.

e.g.

```
"tinyint_1 = 100"
```

### schema [config]

#### fields [Config]

The schema of the starRocks that you want to generate

e.g.

```
schema {
    fields {
        name = string
        age = int
    }
  }
```

### request_tablet_size [int]

The number of StarRocks Tablets corresponding to an Partition. The smaller this value is set, the more partitions will be generated. This will increase the parallelism on the engine side, but at the same time will cause greater pressure on StarRocks.

The following is an example to explain how to use request_tablet_size to controls the generation of partitions

```
the tablet distribution of StarRocks table in cluster as follower

be_node_1 tablet[1, 2, 3, 4, 5]
be_node_2 tablet[6, 7, 8, 9, 10]
be_node_3 tablet[11, 12, 13, 14, 15]

1.If not set request_tablet_size, there will no limit on the number of tablets in a single partition. The partitions will be generated as follows  

partition[0] read data of tablet[1, 2, 3, 4, 5] from be_node_1 
partition[1] read data of tablet[6, 7, 8, 9, 10] from be_node_2 
partition[2] read data of tablet[11, 12, 13, 14, 15] from be_node_3 

2.if set request_tablet_size=3, the limit on the number of tablets in a single partition is 3. The partitions will be generated as follows

partition[0] read data of tablet[1, 2, 3] from be_node_1 
partition[1] read data of tablet[4, 5] from be_node_1 
partition[2] read data of tablet[6, 7, 8] from be_node_2 
partition[3] read data of tablet[9, 10] from be_node_2 
partition[4] read data of tablet[11, 12, 13] from be_node_3 
partition[5] read data of tablet[14, 15] from be_node_3 
```

### scan_connect_timeout_ms [int]

requests connection timeout sent to StarRocks

### scan_query_timeout_sec [int]

Query the timeout time of StarRocks, the default value is 1 hour, -1 means no timeout limit

### scan_keep_alive_min [int]

The keep-alive duration of the query task, in minutes. The default value is 10. we recommend that you set this parameter to a value greater than or equal to 5.

### scan_batch_rows [int]

The maximum number of data rows to read from BE at a time. Increasing this value reduces the number of connections established between engine and StarRocks and therefore mitigates overhead caused by network latency.

### scan_mem_limit [long]

The maximum memory space allowed for a single query in the BE node, in bytes. The default value is 2147483648 (2 GB).

### max_retries [int]

number of retry requests sent to StarRocks

### scan.params. [string]

The parameter of the scan data from be

## Example

```
source {
  StarRocks {
    nodeUrls = ["starrocks_e2e:8030"]
    username = root
    password = ""
    database = "test"
    table = "e2e_table_source"
    scan_batch_rows = 10
    max_retries = 3
    schema {
        fields {
           BIGINT_COL = BIGINT
           LARGEINT_COL = STRING
           SMALLINT_COL = SMALLINT
           TINYINT_COL = TINYINT
           BOOLEAN_COL = BOOLEAN
           DECIMAL_COL = "DECIMAL(20, 1)"
           DOUBLE_COL = DOUBLE
           FLOAT_COL = FLOAT
           INT_COL = INT
           CHAR_COL = STRING
           VARCHAR_11_COL = STRING
           STRING_COL = STRING
           DATETIME_COL = TIMESTAMP
           DATE_COL = DATE
        }
    }
    scan.params.scanner_thread_pool_thread_num = "3"
    
  }
}
```

## Changelog

### next version

- Add StarRocks Source Connector

