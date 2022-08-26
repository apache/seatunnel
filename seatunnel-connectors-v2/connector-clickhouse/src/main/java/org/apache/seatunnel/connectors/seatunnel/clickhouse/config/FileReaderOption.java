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

public class FileReaderOption implements Serializable {

    private ShardMetadata shardMetadata;
    private Map<String, String> tableSchema;
    private List<String> fields;
    private String clickhouseLocalPath;
    private ClickhouseFileCopyMethod copyMethod;
    private boolean nodeFreePass;
    private Map<String, String> nodeUser;
    private Map<String, String> nodePassword;
    private SeaTunnelRowType seaTunnelRowType;

    public FileReaderOption(ShardMetadata shardMetadata, Map<String, String> tableSchema,
                            List<String> fields, String clickhouseLocalPath,
                            ClickhouseFileCopyMethod copyMethod,
                            Map<String, String> nodeUser,
                            Map<String, String> nodePassword) {
        this.shardMetadata = shardMetadata;
        this.tableSchema = tableSchema;
        this.fields = fields;
        this.clickhouseLocalPath = clickhouseLocalPath;
        this.copyMethod = copyMethod;
        this.nodeUser = nodeUser;
        this.nodePassword = nodePassword;
    }

    public SeaTunnelRowType getSeaTunnelRowType() {
        return seaTunnelRowType;
    }

    public void setSeaTunnelRowType(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    public boolean isNodeFreePass() {
        return nodeFreePass;
    }

    public void setNodeFreePass(boolean nodeFreePass) {
        this.nodeFreePass = nodeFreePass;
    }

    public String getClickhouseLocalPath() {
        return clickhouseLocalPath;
    }

    public void setClickhouseLocalPath(String clickhouseLocalPath) {
        this.clickhouseLocalPath = clickhouseLocalPath;
    }

    public ClickhouseFileCopyMethod getCopyMethod() {
        return copyMethod;
    }

    public void setCopyMethod(ClickhouseFileCopyMethod copyMethod) {
        this.copyMethod = copyMethod;
    }

    public Map<String, String> getNodeUser() {
        return nodeUser;
    }

    public void setNodeUser(Map<String, String> nodeUser) {
        this.nodeUser = nodeUser;
    }

    public Map<String, String> getNodePassword() {
        return nodePassword;
    }

    public void setNodePassword(Map<String, String> nodePassword) {
        this.nodePassword = nodePassword;
    }

    public ShardMetadata getShardMetadata() {
        return shardMetadata;
    }

    public void setShardMetadata(ShardMetadata shardMetadata) {
        this.shardMetadata = shardMetadata;
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
}
