# Qdrant

> Qdrant Sink Connector

## Description

[Qdrant](https://qdrant.tech/) is a high-performance vector search engine and vector database.

This connector can be used to write data into a Qdrant collection.

## Data Type Mapping

| SeaTunnel Data Type | Qdrant Data Type |
|---------------------|------------------|
| TINYINT             | INTEGER          |
| SMALLINT            | INTEGER          |
| INT                 | INTEGER          |
| BIGINT              | INTEGER          |
| FLOAT               | DOUBLE           |
| DOUBLE              | DOUBLE           |
| BOOLEAN             | BOOL             |
| STRING              | STRING           |
| ARRAY               | LIST             |
| FLOAT_VECTOR        | DENSE_VECTOR     |
| BINARY_VECTOR       | DENSE_VECTOR     |
| FLOAT16_VECTOR      | DENSE_VECTOR     |
| BFLOAT16_VECTOR     | DENSE_VECTOR     |
| SPARSE_FLOAT_VECTOR | SPARSE_VECTOR    |

The value of the primary key column will be used as point ID in Qdrant. If no primary key is present, a random UUID will be used.

## Options

|      name       |  type  | required | default value |
|-----------------|--------|----------|---------------|
| collection_name | string | yes      | -             |
| batch_size      | int    | no       | 64            |
| host            | string | no       | localhost     |
| port            | int    | no       | 6334          |
| api_key         | string | no       | -             |
| use_tls         | int    | no       | false         |
| common-options  |        | no       | -             |

### collection_name [string]

The name of the Qdrant collection to read data from.

### batch_size [int]

The batch size of each upsert request to Qdrant.

### host [string]

The host name of the Qdrant instance. Defaults to "localhost".

### port [int]

The gRPC port of the Qdrant instance.

### api_key [string]

The API key to use for authentication if set.

### use_tls [bool]

Whether to use TLS(SSL) connection. Required if using Qdrant cloud(https).

### common options

Sink plugin common parameters, please refer to [Source Common Options](../sink-common-options.md) for details.
