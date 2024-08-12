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

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.timeplus.shard.ShardMetadata;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class FileReaderOption implements Serializable {

    private ShardMetadata shardMetadata;
    private Map<String, String> tableSchema;
    private List<String> fields;
    private String timeplusLocalPath;
    private TimeplusFileCopyMethod copyMethod;
    private boolean nodeFreePass;
    private Map<String, String> nodeUser;
    private Map<String, String> nodePassword;
    private SeaTunnelRowType seaTunnelRowType;
    private boolean compatibleMode;
    private String fileTempPath;
    private String fileFieldsDelimiter;

    public FileReaderOption(
            ShardMetadata shardMetadata,
            Map<String, String> tableSchema,
            List<String> fields,
            String timeplusLocalPath,
            TimeplusFileCopyMethod copyMethod,
            Map<String, String> nodeUser,
            boolean nodeFreePass,
            Map<String, String> nodePassword,
            boolean compatibleMode,
            String fileTempPath,
            String fileFieldsDelimiter) {
        this.shardMetadata = shardMetadata;
        this.tableSchema = tableSchema;
        this.fields = fields;
        this.timeplusLocalPath = timeplusLocalPath;
        this.copyMethod = copyMethod;
        this.nodeUser = nodeUser;
        this.nodeFreePass = nodeFreePass;
        this.nodePassword = nodePassword;
        this.compatibleMode = compatibleMode;
        this.fileFieldsDelimiter = fileFieldsDelimiter;
        this.fileTempPath = fileTempPath;
    }
}
