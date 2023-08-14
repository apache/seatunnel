# Access

> Access source connector

## Description

Read data from Access.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name              |  type  | required | default value |
|-------------------|--------|----------|---------------|
| driver            | String | Yes      | -             |
| url               | String | Yes      | -             |
| username          | String | Yes      | -             |
| password          | String | yes      | -             |
| query             | String | yes      | -             |

### driver [string]

The jdbc class name used to connect to the remote data source , Here we fill in  `net.ucanaccess.jdbc.UcanaccessDriver`.

### url [string]

The URL of the JDBC connection. Refer to a case: jdbc:ucanaccess://D:\\geely\\code\\access-test\\access.accdb（windows）
The URL of the JDBC connection. Refer to a case: jdbc:ucanaccess:/opt/download/youdir/access.accdb（unix/linux）

### username [string]

`Access` user username.

### password [string]

`Access` user password.

### query [String]

Query statement

## Examples

```hocon
source {
  Access {
    driver = "net.ucanaccess.jdbc.UcanaccessDriver"
    url = "jdbc:ucanaccess:/opt/download/youdir/access.accdb"
    username = ""
    password = ""
    query = "select *  from access_test"
  }
}
```

## Changelog

### next version

- Add Access Source Connector

