# Milvus

> Milvus sink connector

## Description

Write data to Apache milvus.

## Key features

- [ ] [exactly-once](file:///Users/liugddx/code/incubator-seatunnel/docs/en/concept/connector-v2-features.md)

## Options

|       name        |  type  | required |     default value      |
|-------------------|--------|----------|------------------------|
| milvus_host       | String | Yes      | -                      |
| milvus_port       | Int    | No       | 19530                  |
| username          | String | Yes      | -                      |
| password          | String | Yes      | -                      |
| collection_name   | String | No       | -                      |
| partition_field   | String | No       | -                      |
| openai_engine     | String | No       | text-embedding-ada-002 |
| openai_api_key    | String | No       | -                      |
| embeddings_fields | String | No       | -                      |

### milvus_host [string]

The milvus host.

### milvus_port [int]

This port is for gRPC. Default is 19530.

### username [String]

The username of milvus server.

### password [String]

The password of milvus server.

### collection_name [String]

A collection of milvus, which is similar to a table in a relational database.

### partition_field [String]

Partition fields, which must be included in the collection's schema.

### openai_engine [String]

Text embedding model. Default is 'text-embedding-ada-002'.

### openai_api_key [String]

Use your own Open AI API Key here.

### embeddings_fields [String]

Fields to be embedded,They use`,`for splitting.

## Examples

```hocon
sink {
  Milvus {
    milvus_host = localhost
    milvus_port = 19530
    username = root
    password = Milvus
    collection_name = title_db
    openai_engine = text-embedding-ada-002
    openai_api_key = sk-xxxx
    embeddings_fields = title_2
  }
```

## Changelog

### next version

- Add Milvus Sink Connector

