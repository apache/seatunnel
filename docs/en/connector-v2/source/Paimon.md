# Paimon

> Paimon source connector

## Description

Read data from Apache Paimon.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

|      name      |  type  | required | default value |
|----------------|--------|----------|---------------|
| warehouse      | String | Yes      | -             |
| database       | String | Yes      | -             |
| table          | String | Yes      | -             |
| hdfs_site_path | String | No       | -             |
| projection     | String | No       | -             |

### warehouse [string]

Paimon warehouse path

### database [string]

The database you want to access

### table [string]

The table you want to access

### hdfs_site_path [string]

The file path of `hdfs-site.xml`

### projection [string]

Decides which columns of the paimon table to read. For multiple columns you need to use "," for split, such as "3,2".

## Examples

```hocon
source {
 Paimon {
     warehouse = "/tmp/paimon"
     database = "default"
     table = "st_test"
   }
}
```

## Changelog

### next version

- Add Paimon Source Connector
- Support projection for Paimon Source

