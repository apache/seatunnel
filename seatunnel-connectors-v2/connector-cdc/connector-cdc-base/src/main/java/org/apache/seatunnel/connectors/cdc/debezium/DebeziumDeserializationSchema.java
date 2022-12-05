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

package org.apache.seatunnel.connectors.cdc.debezium;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;

/**
 * The deserialization schema describes how to turn the Debezium SourceRecord into data types
 * (Java/Scala objects) that are processed by engine.
 *
 * @param <T> The type created by the deserialization schema.
 */
public interface DebeziumDeserializationSchema<T> extends Serializable {

    /** Deserialize the Debezium record, it is represented in Kafka {@link SourceRecord}. */
    void deserialize(SourceRecord record, Collector<T> out) throws Exception;

    SeaTunnelDataType<T> getProducedType();
}
