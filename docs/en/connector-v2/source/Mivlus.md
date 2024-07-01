# Milvus

> Milvus source connector

## Description

Read data from Milvus or Zilliz Cloud

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)

## Source Options

|    Name    |  Type  | Required | Default |                                        Description                                         |
|------------|--------|----------|---------|--------------------------------------------------------------------------------------------|
| url        | String | Yes      | -       | The URL to connect to Milvus or Zilliz Cloud.                                              |
| token      | String | Yes      | -       | User:password                                                                              |
| database   | String | Yes      | default | Read data from which database.                                                             |
| collection | String | No       | -       | If set, will only read one collection, otherwise will read all collections under database. |

## Task Example

```bash
source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
  }
}
```

