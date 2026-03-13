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

import java.io.Serializable;

public class PulsarConsumerMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final CatalogTable catalogTable;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private final PulsarDiscoverer discoverer;
    private final StartCursor startCursor;
    private final StopCursor stopCursor;
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

    public TablePath getTablePath() {
        return tablePath;
    }

    public CatalogTable getCatalogTable() {
        return catalogTable;
    }

    public DeserializationSchema<SeaTunnelRow> getDeserializationSchema() {
        return deserializationSchema;
    }

    public PulsarDiscoverer getDiscoverer() {
        return discoverer;
    }

    public StartCursor getStartCursor() {
        return startCursor;
    }

    public StopCursor getStopCursor() {
        return stopCursor;
    }

    public PulsarConsumerConfig getConsumerConfig() {
        return consumerConfig;
    }
}
