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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.streaming.api.datastream.DataStream;

import lombok.Data;

import java.util.List;

@Data
public class DataStreamTableInfo {

    private DataStream<SeaTunnelRow> dataStream;

    private List<CatalogTable> catalogTables;

    private String tableName;

    /** Whether this stream carries Flink's internal schema-evolution control protocol. */
    private boolean schemaEvolutionEnabled;

    public DataStreamTableInfo(
            DataStream<SeaTunnelRow> dataStream,
            List<CatalogTable> catalogTables,
            String tableName) {
        this(dataStream, catalogTables, tableName, false);
    }

    public DataStreamTableInfo(
            DataStream<SeaTunnelRow> dataStream,
            List<CatalogTable> catalogTables,
            String tableName,
            boolean schemaEvolutionEnabled) {
        this.dataStream = dataStream;
        this.catalogTables = catalogTables;
        this.tableName = tableName;
        this.schemaEvolutionEnabled = schemaEvolutionEnabled;
    }
}
