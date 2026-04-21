/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.adapter;

import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumTopicNaming;

import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

public class PostgresTopicNamingAdapter implements DebeziumTopicNaming<TableId> {

    private final TopicSelector<TableId> delegate;
    private final String heartbeatPrefix;

    public PostgresTopicNamingAdapter(TopicSelector<TableId> delegate, String heartbeatPrefix) {
        this.delegate = delegate;
        this.heartbeatPrefix = heartbeatPrefix;
    }

    @Override
    public String getPrimaryTopic() {
        return delegate.getPrimaryTopic();
    }

    @Override
    public String getHeartbeatTopic() {
        return heartbeatPrefix;
    }

    @Override
    public String dataChangeTopicName(TableId tableId) {
        return delegate.topicNameFor(tableId);
    }

    public TopicSelector<TableId> getDelegate() {
        return delegate;
    }
}
