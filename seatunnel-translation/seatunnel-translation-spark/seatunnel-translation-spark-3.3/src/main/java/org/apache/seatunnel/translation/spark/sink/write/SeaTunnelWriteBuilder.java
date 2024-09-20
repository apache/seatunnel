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

package org.apache.seatunnel.translation.spark.sink.write;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class SeaTunnelWriteBuilder<StateT, CommitInfoT, AggregatedCommitInfoT>
        implements WriteBuilder {

    private final SeaTunnelSink<SeaTunnelRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink;
    private final Map<String, String> properties;
    private final CatalogTable[] catalogTables;
    private final StructType schema;
    private final String checkpointLocation;
    private final String jobId;

    public SeaTunnelWriteBuilder(
            SeaTunnelSink<SeaTunnelRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            Map<String, String> properties,
            CatalogTable[] catalogTables,
            StructType schema,
            String checkpointLocation,
            String jobId) {
        this.sink = sink;
        this.properties = properties;
        this.catalogTables = catalogTables;
        this.schema = schema;
        this.checkpointLocation = checkpointLocation;
        this.jobId = jobId;
    }

    @Override
    public Write build() {
        return new SeaTunnelWrite<>(
                sink, properties, catalogTables, schema, checkpointLocation, jobId);
    }
}
