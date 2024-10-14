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

package org.apache.seatunnel.connectors.seatunnel.timeplus.config;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.timeplus.shard.ShardMetadata;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

@Builder
@Getter
public class ReaderOption implements Serializable {

    @Setter private ShardMetadata shardMetadata;
    private String[] primaryKeys;
    private boolean allowExperimentalLightweightDelete;
    private boolean supportUpsert;
    private SchemaSaveMode schemaSaveMode;
    private DataSaveMode dataSaveMode;
    private String tableName; // the name after replacing the placeholder
    private String tableEngine;
    @Setter private Map<String, String> tableSchema;
    @Setter private SeaTunnelRowType seaTunnelRowType;
    private Properties properties;
    private int bulkSize;
}
