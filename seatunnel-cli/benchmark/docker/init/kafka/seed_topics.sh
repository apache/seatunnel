#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Creates benchmark topics and seeds sample messages so streaming source
# tasks have data to consume immediately.

set -e
BROKER="kafka:19092"
KBIN="/opt/kafka/bin"

# source topics (seeded below) + sink topics used by fan-out/routing tasks
topics=(clicks order_events dbz.shop.users events wms.inventory
        shop.orders.changelog clicks_mirror clicks_copy_a clicks_copy_b
        vip_clicks other_clicks events_mirror inventory.changelog)
for t in "${topics[@]}"; do
    "$KBIN/kafka-topics.sh" --bootstrap-server "$BROKER" --create --if-not-exists \
        --topic "$t" --partitions 1 --replication-factor 1
done

# clicks: plain JSON events
"$KBIN/kafka-console-producer.sh" --bootstrap-server "$BROKER" --topic clicks <<'EOF'
{"event_id": "e1", "user_id": 1, "url": "/home", "ts": "2026-01-01T00:00:00"}
{"event_id": "e2", "user_id": 2, "url": "/pricing", "ts": "2026-01-01T00:01:00"}
{"event_id": "e3", "user_id": 1, "url": "/product/1", "ts": "2026-01-01T00:02:00"}
EOF

# order_events: string-typed fields that need casting
"$KBIN/kafka-console-producer.sh" --bootstrap-server "$BROKER" --topic order_events <<'EOF'
{"order_id": "o-1001", "amount": "25.50", "created_at": "2026-01-05 10:00:00"}
{"order_id": "o-1002", "amount": "1200.00", "created_at": "2026-01-06 11:30:00"}
EOF

# events: generic id/name/ts messages
"$KBIN/kafka-console-producer.sh" --bootstrap-server "$BROKER" --topic events <<'EOF'
{"id": 1, "name": "alpha", "ts": "2026-01-01T00:00:00"}
{"id": 2, "name": "beta", "ts": "2026-01-01T00:05:00"}
EOF

# dbz.shop.users: debezium_json envelope
"$KBIN/kafka-console-producer.sh" --bootstrap-server "$BROKER" --topic dbz.shop.users <<'EOF'
{"before": null, "after": {"id": 1, "name": "alice", "email": "alice@example.com"}, "source": {"db": "shop", "table": "users"}, "op": "c", "ts_ms": 1767225600000}
{"before": null, "after": {"id": 2, "name": "bob", "email": "bob@example.com"}, "source": {"db": "shop", "table": "users"}, "op": "c", "ts_ms": 1767225660000}
EOF

echo "Kafka topics seeded."
