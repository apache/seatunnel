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

import ChangeLog from '../changelog/connector-http-azure-iot-hub.md';

# AzureIoTHub

The Azure IoT Hub sink sends one device-to-cloud telemetry message per INSERT row from a registered device. It uses the [Send Device Event REST API](https://learn.microsoft.com/en-us/rest/api/iothub/device/device/send-device-event?view=rest-iothub-device-2021-11-30) over HTTPS. This connector produces telemetry only. Its end-to-end suite targets SeaTunnel Zeta.

Register the device in your IoT hub with symmetric-key authentication and supply one of its Base64 keys. The sink generates a device-scoped SAS token for each attempt. It uses the worker JVM's trusted certificate authorities and validates the endpoint hostname.

## Sink options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `hub_url` | String | Yes | | Trusted HTTPS origin, for example `https://my-hub.azure-devices.net`. An optional port and trailing slash are allowed. User information, endpoint paths, queries, and fragments are invalid. |
| `device_id` | String | Yes | | Case-sensitive registered device ID, up to 128 characters. |
| `secret_key` | String | Yes | | Registered device symmetric key, with 16–128 bytes after Base64 decoding. Inject it from a secret store or environment variable. |
| `format` | String | No | `JSON` | `JSON` encodes the entire row as one UTF-8 JSON object. `TEXT` emits a UTF-8 string and `BINARY` emits raw bytes. Format names are case-insensitive. |
| `field` | String | For `TEXT` and `BINARY` | | The input STRING column for `TEXT` or BYTES column for `BINARY`. The selected value must be non-null. Empty strings and byte arrays are valid. |
| `application_properties` | Map&lt;String, String&gt; | No | `{}` | Public message properties, suitable for routing. Up to 32 names of 1–64 ASCII letters, digits, underscores, periods, or hyphens. Names are normalized to lowercase and must be distinct ignoring case. Each value supports up to 1,024 characters from the [HTTPS message-property alphabet](https://learn.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-messages-construct): ASCII letters/digits and <code>!#$%&amp;'*+-.^_&#96;&#124;~</code>. Spaces, slash and colon are rejected before network use. Names and values together may use up to 4,096 bytes. |
| `sas_ttl_seconds` | Integer | No | `3600` | Lifetime in seconds of each generated SAS token. Range 30–86,400. |
| `max_payload_bytes` | Integer | No | `262144` | Maximum body size in bytes. Range 1–262,144. The effective limit also reserves space for system and application properties within the IoT Hub 256 KiB message limit. |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connect timeout in milliseconds. Range 1–300,000. |
| `read_timeout_ms` | Integer | No | `10000` | TLS handshake and response read inactivity timeout in milliseconds. Range 1–300,000. |
| `row_timeout_ms` | Integer | No | `30000` | End-to-end I/O budget per row in milliseconds, including connection, TLS, writing, reading, and any retry delay. Range 1–300,000. |
| `max_attempts` | Integer | No | `3` | Maximum HTTP attempts per row including the initial attempt. Range 1–10. |

JSON conversion also uses the effective body cap as a work budget before allocating its tree: every logical value/container occurrence costs one unit, plus field-name/string UTF-8 bytes, Base64 body bytes and numeric representation work. Shared values and overwritten map entries are charged each time. Extremely large or repeatedly normalized inputs can therefore be rejected even when their final JSON would be smaller. Schema checks, conversion and container output are iterative, with no arbitrary fixed depth limit. Exact escaped output bytes are checked separately. TEXT and BINARY retain their existing byte and type checks.

## Delivery behavior

The sink waits for HTTP 204 before completing a row. Only HTTP 429 is retried. A valid `Retry-After` supplies a minimum wait, otherwise the sink uses a bounded backoff when the header is missing or unusable. A retry must fit the remaining row budget. Redirects, other HTTP responses, and transport failures fail the row. After a lost acknowledgement, IoT Hub may already have accepted the message.

The sink stores no pending batches or transactions in checkpoints. Rows replayed after a job restart may produce duplicates. Route or deduplicate downstream with an application identifier when required.

## Example

The following job sends JSON telemetry from a registered device named `sensor-1`. Provide `AZURE_IOT_DEVICE_KEY` through your deployment's secret injection.

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 1
    schema {
      fields {
        reading = double
        unit = string
      }
    }
    rows = [{ kind = INSERT, fields = [21.5, "C"] }]
  }
}

sink {
  AzureIoTHub {
    hub_url = "https://my-hub.azure-devices.net"
    device_id = "sensor-1"
    secret_key = ${AZURE_IOT_DEVICE_KEY}
    application_properties = {
      measurement = "temperature"
    }
  }
}
```

<ChangeLog />
