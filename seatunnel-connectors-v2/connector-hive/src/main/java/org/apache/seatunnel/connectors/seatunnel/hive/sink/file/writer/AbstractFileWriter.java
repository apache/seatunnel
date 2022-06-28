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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.HiveSinkConfig;

import lombok.NonNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractFileWriter implements FileWriter {
    protected Map<String, String> needMoveFiles;
    protected SeaTunnelRowType seaTunnelRowType;
    protected long jobId;
    protected int subTaskIndex;
    protected HiveSinkConfig hiveSinkConfig;

    private static final String SEATUNNEL = "seatunnel";
    private static final String NON_PARTITION = "NON_PARTITION";

    protected Map<String, String> beingWrittenFile;

    protected String checkpointId;
    protected final int[] partitionKeyIndexes;

    public AbstractFileWriter(@NonNull SeaTunnelRowType seaTunnelRowType,
                              @NonNull HiveSinkConfig hiveSinkConfig,
                              long jobId,
                              int subTaskIndex) {
        checkArgument(jobId > 0);
        checkArgument(subTaskIndex > -1);

        this.needMoveFiles = new HashMap<>();
        this.seaTunnelRowType = seaTunnelRowType;
        this.jobId = jobId;
        this.subTaskIndex = subTaskIndex;
        this.hiveSinkConfig = hiveSinkConfig;

        this.beingWrittenFile = new HashMap<>();
        if (this.hiveSinkConfig.getPartitionFieldNames() == null) {
            this.partitionKeyIndexes = new int[0];
        } else {
            this.partitionKeyIndexes = IntStream.range(0, seaTunnelRowType.getTotalFields())
                .filter(i -> hiveSinkConfig.getPartitionFieldNames().contains(seaTunnelRowType.getFieldName(i)))
                .toArray();
        }
    }

    public String getOrCreateFilePathBeingWritten(@NonNull SeaTunnelRow seaTunnelRow) {
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

    private String getBeingWrittenFileKey(@NonNull SeaTunnelRow seaTunnelRow) {
        if (partitionKeyIndexes.length > 0) {
            return Arrays.stream(partitionKeyIndexes)
                .boxed()
                .map(i -> seaTunnelRowType.getFieldName(i) + "=" + seaTunnelRow.getField(i))
                .collect(Collectors.joining("/"));
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
    @NonNull
    public abstract String getFileSuffix();

    public String getHiveLocation(@NonNull String seaTunnelFilePath) {
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

    @Override
    public void resetFileWriter(@NonNull String checkpointId) {
        this.checkpointId = checkpointId;
        this.needMoveFiles = new HashMap<>();
        this.beingWrittenFile = new HashMap<>();
        this.resetMoreFileWriter(checkpointId);
    }

    public abstract void resetMoreFileWriter(@NonNull String checkpointId);

    @Override
    public void abort() {
        this.needMoveFiles = new HashMap<>();
        this.beingWrittenFile = new HashMap<>();
        this.abortMore();
    }

    public abstract void abortMore();
}
