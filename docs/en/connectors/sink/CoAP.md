<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

import ChangeLog from '../changelog/connector-coap.md';

# CoAP

The `CoAP` sink starts one UDP exchange per input row and waits for a final 2.xx response. The URL identifies a single unicast `coap://` destination. Use this connector on a network appropriate for unauthenticated and unencrypted traffic.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `url` | String | — | Required target `coap://` URI of a unicast UDP endpoint. |
| `method` | Enum | `POST` | Request method. Accepts `GET`, `POST`, `PUT` or `DELETE`. |
| `request_type` | Enum | `NON` | `CON` sends a confirmable message. `NON` sends a non-confirmable message. |
| `request_timeout_ms` | Integer | `5000` | Maximum budget per row, from 1 to 120000 milliseconds. |
| `format` | Enum | `JSON` | Supports `JSON`, `TEXT`, `BINARY`, `CSV`, `AVRO`, `PROTOBUF`, and `RECORD_JSON`. |
| `max_payload_bytes` | Integer | `65536` | Maximum assembled body bytes, from 16 to 1048576. |
| `blockwise_timeout_ms` | Integer | `30000` | Partial block transfer inactivity limit, from 500 to 120000 milliseconds. |
| `csv_delimiter` | String | `,` | One printable non-quote delimiter for CSV. |
| `protobuf_schema` | String | — | Protobuf schema content, required when `format = PROTOBUF`. |
| `protobuf_message_name` | String | — | Fully qualified message name, required when `format = PROTOBUF`. |
| `payload_field` | String | `payload` | Input `STRING` or `BYTES` column for `TEXT` or `BINARY`. |
| `avro_container` | Boolean | `true` | Write one row in a self-describing Avro object container. Set to `false` to send one raw datum. |

## Requests and payloads

GET and DELETE send an empty request. POST and PUT use the selected representation.

| Format | Content-Format | Representation |
| --- | --- | --- |
| `JSON` | 50 | Valid UTF-8 JSON. A source without `schema` exposes the JSON text as `payload`. A source with `schema` expects an object and emits typed fields. A sink encodes the input row as an object. |
| `TEXT` | 0 | One strict UTF-8 string, including an empty string. |
| `BINARY` | 42 | One byte array, including zero bytes. |
| `CSV` | 0 | One delimited record, without a header. |
| `AVRO` | 42 | A self-describing object container or one raw datum using the schema generated from the input row type. |
| `PROTOBUF` | 42 | One binary message using the supplied schema and message name. |
| `RECORD_JSON` | 50 | A JSON record with a header and type-tagged fields built from the input row. |

Protocol retransmissions of a CON request keep the same exchange identity. A NON request does not request a protocol acknowledgement. Both modes require a final successful response for the row to succeed. RST, a non-2.xx response, and expiry of the request budget fail the write. A timeout can occur after remote side effects, so the connector never starts a replacement exchange for an ambiguous response.

Each writer processes one request at a time and has no pending local records once a write completes. Checkpoints and end-of-input complete after the preceding writes. Recovery of an upstream job can send a row again. An application response gives the remote peer's acknowledgement and does not establish durable or transactional delivery.

## Example

```hocon
sink {
  CoAP {
    url = "coap://device.example:5683/sensor/data"
    method = "PUT"
    request_type = "CON"
    format = "JSON"
    request_timeout_ms = 5000
  }
}
```

<ChangeLog />
