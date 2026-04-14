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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConsumerConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.start.StartCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop.StopCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.PulsarDiscoverer;

import lombok.Getter;

import java.io.Serializable;

/**
 * Runtime metadata container for each table in a Pulsar source configuration.
 *
 * <p>This class is different from {@code PulsarTableConfig}:
 *
 * <ul>
 *   <li>{@code PulsarTableConfig} is used during configuration parsing and validation
 *   <li>{@code PulsarConsumerMetadata} is used during runtime and includes constructed objects like
 *       {@link DeserializationSchema}
 * </ul>
 *
 * <p><b>Thread-safety:</b> This class is immutable and thread-safe. All fields are final and can be
 * safely accessed by multiple threads.
 *
 * <p><b>Serialization:</b> This class is {@link Serializable} and can be transferred between
 * enumerator and reader. However, {@link DeserializationSchema} implementations must also be
 * serializable.
 *
 * <p><b>Lifecycle:</b>
 *
 * <ol>
 *   <li>Created by {@link PulsarSource#createConsumerMetadata}
 *   <li>Stored in {@code Map<TablePath, PulsarConsumerMetadata>} in {@link PulsarSource}
 *   <li>Passed to {@code PulsarSourceReader} and {@code PulsarSplitEnumerator}
 *   <li>Used to resolve table-specific deserializers, cursors, and discoverers
 * </ol>
 */
@Getter
public class PulsarConsumerMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Logical table identifier (e.g., "db.orders"). Never null. */
    private final TablePath tablePath;

    /** Catalog table including schema information. */
    private final CatalogTable catalogTable;

    /**
     * Deserialization schema for this table's data format. Different tables may use different
     * formats (e.g., JSON vs CANAL_JSON).
     */
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    /** Partition discoverer for this table (topic or topic-pattern). */
    private final PulsarDiscoverer discoverer;

    /** Start cursor (initial position) for this table's subscription. */
    private final StartCursor startCursor;

    /**
     * Stop cursor (end position) for this table's subscription. Determines if the table is bounded
     * or unbounded.
     */
    private final StopCursor stopCursor;

    /** Consumer configuration including subscription name. */
    private final PulsarConsumerConfig consumerConfig;

    public PulsarConsumerMetadata(
            TablePath tablePath,
            CatalogTable catalogTable,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            PulsarDiscoverer discoverer,
            StartCursor startCursor,
            StopCursor stopCursor,
            PulsarConsumerConfig consumerConfig) {
        this.tablePath = tablePath;
        this.catalogTable = catalogTable;
        this.deserializationSchema = deserializationSchema;
        this.discoverer = discoverer;
        this.startCursor = startCursor;
        this.stopCursor = stopCursor;
        this.consumerConfig = consumerConfig;
    }
}
