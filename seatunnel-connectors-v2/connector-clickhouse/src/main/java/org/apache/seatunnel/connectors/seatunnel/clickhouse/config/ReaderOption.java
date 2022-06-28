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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.config;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.ShardMetadata;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ReaderOption implements Serializable {

    private ShardMetadata shardMetadata;
    private List<String> fields;

    private Map<String, String> tableSchema;
    private SeaTunnelRowType seaTunnelRowType;
    private Properties properties;
    private int bulkSize;

    public ReaderOption(ShardMetadata shardMetadata,
                        Properties properties, List<String> fields, Map<String, String> tableSchema, int bulkSize) {
        this.shardMetadata = shardMetadata;
        this.properties = properties;
        this.fields = fields;
        this.tableSchema = tableSchema;
        this.bulkSize = bulkSize;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public ShardMetadata getShardMetadata() {
        return shardMetadata;
    }

    public void setShardMetadata(ShardMetadata shardMetadata) {
        this.shardMetadata = shardMetadata;
    }

    public SeaTunnelRowType getSeaTunnelRowType() {
        return seaTunnelRowType;
    }

    public void setSeaTunnelRowType(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    public Map<String, String> getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(Map<String, String> tableSchema) {
        this.tableSchema = tableSchema;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
    }
}
