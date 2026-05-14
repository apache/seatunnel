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

---
source: Kafka
sink: Clickhouse
description: Kafka source to Clickhouse sink (streaming)
notes: Streaming mode. Kafka needs schema when format=json.
---

## Source Template

```hocon
Kafka {
    bootstrap.servers = "kafka:9092"
    topic = "{topic}"
    format = "json"
    start_mode = "latest"
    plugin_output = "{routing_label}"
    schema {
        fields {
            id = "bigint"
            name = "string"
        }
    }
}
```

## Sink Template

```hocon
Clickhouse {
    plugin_input = "{routing_label}"
    host = "clickhouse:8123"
    database = "default"
    table = "{table}"
    username = "${CLICKHOUSE_USER}"
    password = "${CLICKHOUSE_PASSWORD}"
}
```
