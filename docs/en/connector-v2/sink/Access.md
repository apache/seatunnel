# Access

> Access sink connector

## Description

Write data to Access.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

| name        |  type   | required | default value |
|-------------|---------|----------|---------------|
| driver      | String  | Yes      | -             |
| url         | String  | Yes      | -             |
| username    | String  | Yes      | -             |
| password    | String  | Yes      | -             |
| table       | String  | Yes      |               |
| query       | String  | Yes      |               |

### driver [string]

The jdbc class name used to connect to the remote data source , Here we fill in  `net.ucanaccess.jdbc.UcanaccessDriver`.

### url [string]

The URL of the JDBC connection. Refer to a case: jdbc:ucanaccess://D:\\geely\\code\\access-test\\access.accdb（windows）
The URL of the JDBC connection. Refer to a case: jdbc:ucanaccess:/opt/download/youdir/access.accdb（unix/linux）

### username [string]

`Access` user username.

### password [string]

`Access` user password.

### table [string]

the table you will write message.

### query [String]

Query statement

## Examples

```hocon
sink {
  Access {
    driver = "net.ucanaccess.jdbc.UcanaccessDriver"
    url = "jdbc:ucanaccess:/opt/download/youdir/access.accdb"
    username = ""
    password = ""
    table = "access_test"
    query = """insert into access_test (col1, col2, col3, col4, col5 values(?,?,?,?,?);"""
  }
}
```

## Changelog

### next version

- Add Access Sink Connector
