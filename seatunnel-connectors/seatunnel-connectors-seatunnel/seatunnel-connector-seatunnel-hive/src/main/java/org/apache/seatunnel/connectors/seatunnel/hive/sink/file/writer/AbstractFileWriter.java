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

package org.apache.seatunnel.connectors.seatunnel.hive.sink.file.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.HiveSinkConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractFileWriter implements FileWriter {
    protected Map<String, String> needMoveFiles;
    protected SeaTunnelRowTypeInfo seaTunnelRowTypeInfo;
    protected long jobId;
    protected int subTaskIndex;
    protected HiveSinkConfig hiveSinkConfig;

    private static final String SEATUNNEL = "seatunnel";
    private static final String NON_PARTITION = "NON_PARTITION";

    protected Map<String, String> beingWrittenFile;

    protected String checkpointId;

    public AbstractFileWriter(SeaTunnelRowTypeInfo seaTunnelRowTypeInfo,
                              HiveSinkConfig hiveSinkConfig,
                              long jobId,
                              int subTaskIndex) {
        this.needMoveFiles = new HashMap<>();
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
        this.jobId = jobId;
        this.subTaskIndex = subTaskIndex;
        this.hiveSinkConfig = hiveSinkConfig;

        this.beingWrittenFile = new HashMap<>();
    }

    public String getOrCreateFilePathBeingWritten(SeaTunnelRow seaTunnelRow) {
        String beingWrittenFileKey = getBeingWrittenFileKey(seaTunnelRow);
        // get filePath from beingWrittenFile
        String beingWrittenFilePath = beingWrittenFile.get(beingWrittenFileKey);
        if (beingWrittenFilePath != null) {
            return beingWrittenFilePath;
        } else {
            StringBuilder sbf = new StringBuilder(hiveSinkConfig.getSinkTmpFsRootPath());
            sbf.append("/")
                .append(SEATUNNEL)
                .append("/")
                .append(jobId)
                .append("/")
                .append(checkpointId)
                .append("/")
                .append(hiveSinkConfig.getHiveTableName())
                .append("/")
                .append(beingWrittenFileKey)
                .append("/")
                .append(jobId)
                .append("_")
                .append(subTaskIndex)
                .append(".")
                .append(getFileSuffix());
            String newBeingWrittenFilePath = sbf.toString();
            beingWrittenFile.put(beingWrittenFileKey, newBeingWrittenFilePath);
            return newBeingWrittenFilePath;
        }
    }

    private String getBeingWrittenFileKey(SeaTunnelRow seaTunnelRow) {
        if (this.hiveSinkConfig.getPartitionFieldNames() != null && this.hiveSinkConfig.getPartitionFieldNames().size() > 0) {
            List<String> collect = this.hiveSinkConfig.getPartitionFieldNames().stream().map(partitionKey -> {
                StringBuilder sbd = new StringBuilder(partitionKey);
                sbd.append("=").append(seaTunnelRow.getFieldMap().get(partitionKey));
                return sbd.toString();
            }).collect(Collectors.toList());

            String beingWrittenFileKey = String.join("/", collect);
            return beingWrittenFileKey;
        } else {
            // If there is no partition field in data, We use the fixed value NON_PARTITION as the partition directory
            return NON_PARTITION;
        }
    }

    /**
     * FileWriter need return the file suffix. eg: tex, orc, parquet
     *
     * @return
     */
    public abstract String getFileSuffix();

    public String getHiveLocation(String seaTunnelFilePath) {
        StringBuilder sbf = new StringBuilder(hiveSinkConfig.getSinkTmpFsRootPath());
        sbf.append("/")
            .append(SEATUNNEL)
            .append("/")
            .append(jobId)
            .append("/")
            .append(checkpointId)
            .append("/")
            .append(hiveSinkConfig.getHiveTableName());
        String seaTunnelPath = sbf.toString();
        String tmpPath = seaTunnelFilePath.replaceAll(seaTunnelPath, hiveSinkConfig.getHiveTableFsPath());
        return tmpPath.replaceAll(NON_PARTITION + "/", "");
    }

    public void resetFileWriter(String checkpointId) {
        this.checkpointId = checkpointId;
        this.needMoveFiles = new HashMap<>();
        this.beingWrittenFile = new HashMap<>();
        this.resetMoreFileWriter(checkpointId);
    }

    public abstract void resetMoreFileWriter(String checkpointId);

    public void abort() {
        this.needMoveFiles = new HashMap<>();
        this.beingWrittenFile = new HashMap<>();
        this.abortMore();
    }

    public abstract void abortMore();
}
