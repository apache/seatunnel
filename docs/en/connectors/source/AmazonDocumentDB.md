import ChangeLog from '../changelog/connector-amazondocumentdb.md';

# AmazonDocumentDB

<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

> Amazon DocumentDB source connector

## Support Connector Version

- Amazon DocumentDB clusters compatible with the MongoDB 4.0 and 5.0 APIs

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

Read documents from an existing Amazon DocumentDB collection with a bounded batch scan.

V1 scope for this connector:

- source only
- one database and one collection per source
- explicit schema required; schema inference is out of scope
- optional BSON filter and projection
- one source split; sampling and range-based parallel splitting are out of scope
- sink, CDC/change streams, catalog discovery, and collection creation are out of scope

## Supported DataSource Info

The connector can be installed with `install-plugin.sh` or downloaded from Maven Central.

| Datasource | Supported Versions | Dependency |
| --- | --- | --- |
| Amazon DocumentDB | MongoDB 4.0 and 5.0 compatibility | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-amazondocumentdb) |

## Database Dependency

> Install the connector plugin before running jobs:

```shell
sh bin/install-plugin.sh ${version}
```

Ensure `connector-amazondocumentdb` is included in the plugin installation. The connector uses the MongoDB Java synchronous driver 4.7.1. Amazon DocumentDB is normally reachable only from the VPC that contains the cluster, or through network connectivity to that VPC.

## Data Type Mapping

You must define the SeaTunnel schema explicitly. BSON values are converted according to the configured field types:

| Amazon DocumentDB BSON type | SeaTunnel Data type |
| --- | --- |
| Boolean | BOOLEAN |
| Int32 / Int64 / Double | TINYINT / SMALLINT / INT / BIGINT / FLOAT / DOUBLE |
| Decimal128 | DECIMAL |
| String / ObjectId / Document | STRING |
| Date / Timestamp | DATE / TIME / TIMESTAMP |
| Binary | BYTES |
| Array | ARRAY |
| Document | MAP / ROW |
| Null / Undefined / Decimal128 NaN | null |

Decimal128 values are rounded to the configured scale. If the resulting precision exceeds the
declared DECIMAL precision, conversion fails with an error instead of silently producing `null`.

## Source Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| uri | string | yes | - | MongoDB-compatible connection URI containing the Amazon DocumentDB endpoint and credentials |
| database | string | yes | - | Database name |
| collection | string | yes | - | Collection name |
| schema | config | yes | - | Explicit data schema |
| tls | boolean | no | true | Enable TLS for the connection |
| tls_ca_file | string | yes when `tls=true` | - | Local path to the PEM CA bundle used to verify the cluster certificate |
| match.query | string | no | `{}` | BSON/JSON filter document |
| match.projection | string | no | - | BSON/JSON projection document |
| fetch.size | int | no | 2048 | Number of documents requested from the server per batch; must be greater than zero |
| common-options | - | no | - | Source plugin common parameters, see [Source Common Options](../common-options/source-common-options.md) |

### uri [string]

MongoDB-compatible connection URI. Include the username, password, cluster endpoint, and port. The connector rejects an explicit `retryWrites=true` because Amazon DocumentDB does not support retryable writes, and always applies `retryWrites=false` after parsing the URI.

Example: `mongodb://reader:<password>@sample.cluster-abcdefghijkl.us-east-1.docdb.amazonaws.com:27017/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false`

### database [string]

Name of the existing Amazon DocumentDB database to read.

### collection [string]

Name of the existing collection to read.

### schema [config]

Explicit SeaTunnel schema. Field names are looked up in each BSON document. Missing or BSON null fields become null.

```hocon
schema = {
  fields {
    _id = string
    status = string
    amount = "decimal(18,2)"
    created_at = timestamp
    labels = "map<string,string>"
  }
}
```

### tls [boolean]

Enables TLS. The default is `true`. The connector applies this option after parsing `uri`, so this option determines the final driver TLS setting.

### tls_ca_file [string]

Path to a readable PEM CA bundle. It is required when `tls=true`. The connector creates a connector-local `SSLContext`; it does not modify the JVM-wide `javax.net.ssl.trustStore` system property.

Download the current Amazon trust store from the [AWS documentation](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect_programmatically.html).

### match.query [string]

BSON/JSON filter passed to `find`, for example `{"status": "OPEN"}`. The default `{}` reads all documents.

### match.projection [string]

BSON/JSON projection passed to `find`, for example `{"_id": 1, "status": 1, "amount": 1}`.

### fetch.size [int]

Driver batch-size hint for the server cursor. A larger value can reduce round trips but increases the amount of data buffered by the driver.

### common-options

Source plugin common parameters, refer to [Source Common Options](../common-options/source-common-options.md).

### Tips

> 1. Use a read-only Amazon DocumentDB user and keep credentials out of checked-in job files.<br/>
> 2. TLS is enabled by default. Use the current AWS CA bundle and rotate the local file when AWS updates its trust chain.<br/>
> 3. V1 creates one split. Increasing source parallelism does not parallelize the collection scan.<br/>
> 4. Split state contains the filter and projection, not cursor progress. Recovery restarts the full collection scan from the beginning—even if the failed attempt was almost complete—so downstream writes must be idempotent or use a truncate-and-reload strategy to avoid duplicates.<br/>
> 5. Push selective predicates into `match.query` and include every schema field you need in `match.projection`.

## How to Create an Amazon DocumentDB Data Synchronization Job

The following batch job reads an existing collection and prints rows to the local client:

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  AmazonDocumentDB {
    uri = "mongodb://reader:<password>@sample.cluster-abcdefghijkl.us-east-1.docdb.amazonaws.com:27017/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    database = "app-db"
    collection = "orders"
    tls = true
    tls_ca_file = "/opt/seatunnel/certs/global-bundle.pem"
    fetch.size = 2048
    schema = {
      fields {
        _id = string
        status = string
        amount = "decimal(18,2)"
        created_at = timestamp
      }
    }
  }
}

sink {
  Console {}
}
```

### Filter and project documents

```bash
source {
  AmazonDocumentDB {
    uri = "mongodb://reader:<password>@sample.cluster-abcdefghijkl.us-east-1.docdb.amazonaws.com:27017/?replicaSet=rs0&retryWrites=false"
    database = "app-db"
    collection = "orders"
    tls_ca_file = "/opt/seatunnel/certs/global-bundle.pem"
    match.query = '{"status": "OPEN", "amount": {"$gt": 100}}'
    match.projection = '{"_id": 1, "status": 1, "amount": 1}'
    fetch.size = 512
    schema = {
      fields {
        _id = string
        status = string
        amount = "decimal(18,2)"
      }
    }
  }
}
```

## Changelog

<ChangeLog />
