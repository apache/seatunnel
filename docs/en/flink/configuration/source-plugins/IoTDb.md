# IoTDb

> Source plugin: IoTDb [Flink]

## Description

Read data from IoTDB.

## Options

| name        | type           | required | default value |
| ----------- | -------------- | -------- | ------------- |
| url         | `String`       | yes      | -             |
| user        | `String`       | no       | -             |
| password    | `String`       | no       | -             |
| storage     | `String`       | yes      | -             |
| fields      | `List<String>` | yes      | -             |
| field_types | `List<String>` | yes      | -             |

### url [`String`]

The URL of IoTDB Server.

### user [`String`]

The user of IoTDB Server.

### password [`String`]

The password of IoTDB Server.

### storage [`String`]

The storage name in IoTDB.

### fields [`List<String>`]

The list of Field in IoTDB.

### field_types [`List<String>`]

The list of Field Types in IoTDB.

## Example

### Simple

```hocon
IoTDbSource {
  url = "jdbc:iotdb://127.0.0.1:6667/"
  storage = "root.demo"
  fields = ["s0"]
  field_types = ["INT32"]
}
```

### Auth

```hocon
IoTDbSource {
  url = "jdbc:iotdb://127.0.0.1:6667/"
  user = "root"
  password = "root"
  storage = "root.demo"
  fields = ["s0"]
  field_types = ["INT32"]
}
```
