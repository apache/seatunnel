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

package org.apache.seatunnel.connectors.seatunnel.file.sink.config;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseTextFileConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.Constant;
import org.apache.seatunnel.connectors.seatunnel.file.config.PartitionConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class TextFileSinkConfig extends BaseTextFileConfig implements PartitionConfig {

    private List<String> sinkColumnList;

    private List<String> partitionFieldList;

    private String partitionDirExpression;

    private boolean isPartitionFieldWriteInFile = false;

    private String tmpPath = "/tmp/seatunnel";

    private SaveMode saveMode = SaveMode.ERROR;

    private String fileNameTimeFormat = "yyyy.MM.dd";

    private boolean isEnableTransaction = true;

    //---------------------generator by config params-------------------

    private List<Integer> sinkColumnsIndexInRow;

    private List<Integer> partitionFieldsIndexInRow;

    private int maxRowsInMemory;

    public TextFileSinkConfig(@NonNull Config config, @NonNull SeaTunnelRowType seaTunnelRowTypeInfo) {
        super(config);
        checkArgument(!CollectionUtils.isEmpty(Arrays.asList(seaTunnelRowTypeInfo.getFieldNames())));

        if (config.hasPath(Constant.SINK_COLUMNS) && !CollectionUtils.isEmpty(config.getStringList(Constant.SINK_COLUMNS))) {
            this.sinkColumnList = config.getStringList(Constant.SINK_COLUMNS);
        }

        // if the config sink_columns is empty, all fields in SeaTunnelRowTypeInfo will being write
        if (CollectionUtils.isEmpty(this.sinkColumnList)) {
            this.sinkColumnList = Arrays.asList(seaTunnelRowTypeInfo.getFieldNames());
        }

        if (config.hasPath(Constant.PARTITION_BY)) {
            this.partitionFieldList = config.getStringList(Constant.PARTITION_BY);
        } else {
            this.partitionFieldList = Collections.emptyList();
        }

        if (config.hasPath(Constant.PARTITION_DIR_EXPRESSION) && !StringUtils.isBlank(config.getString(Constant.PARTITION_DIR_EXPRESSION))) {
            this.partitionDirExpression = config.getString(Constant.PARTITION_DIR_EXPRESSION);
        }

        if (config.hasPath(Constant.IS_PARTITION_FIELD_WRITE_IN_FILE)) {
            this.isPartitionFieldWriteInFile = config.getBoolean(Constant.IS_PARTITION_FIELD_WRITE_IN_FILE);
        }

        if (config.hasPath(Constant.TMP_PATH) && !StringUtils.isBlank(config.getString(Constant.TMP_PATH))) {
            this.tmpPath = config.getString(Constant.TMP_PATH);
        }

        if (config.hasPath(Constant.SAVE_MODE) && !StringUtils.isBlank(config.getString(Constant.SAVE_MODE))) {
            this.saveMode = SaveMode.fromStr(config.getString(Constant.SAVE_MODE));
        }

        if (config.hasPath(Constant.FILENAME_TIME_FORMAT) && !StringUtils.isBlank(config.getString(Constant.FILENAME_TIME_FORMAT))) {
            this.fileNameTimeFormat = config.getString(Constant.FILENAME_TIME_FORMAT);
        }

        if (config.hasPath(Constant.IS_ENABLE_TRANSACTION)) {
            this.isEnableTransaction = config.getBoolean(Constant.IS_ENABLE_TRANSACTION);
        }

        if (this.isEnableTransaction && !this.fileNameExpression.contains(Constant.TRANSACTION_EXPRESSION)) {
            throw new RuntimeException("file_name_expression must contains " + Constant.TRANSACTION_EXPRESSION + " when is_enable_transaction is true");
        }

        // check partition field must in seaTunnelRowTypeInfo
        if (!CollectionUtils.isEmpty(this.partitionFieldList)
            && (CollectionUtils.isEmpty(this.sinkColumnList) || !new HashSet<>(this.sinkColumnList).containsAll(this.partitionFieldList))) {
            throw new RuntimeException("partition fields must in sink columns");
        }

        if (!CollectionUtils.isEmpty(this.partitionFieldList) && !isPartitionFieldWriteInFile) {
            if (!this.sinkColumnList.removeAll(this.partitionFieldList)) {
                throw new RuntimeException("remove partition field from sink columns error");
            }
        }

        if (CollectionUtils.isEmpty(this.sinkColumnList)) {
            throw new RuntimeException("sink columns can not be empty");
        }

        Map<String, Integer> columnsMap = new HashMap<>(seaTunnelRowTypeInfo.getFieldNames().length);
        String[] fieldNames = seaTunnelRowTypeInfo.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            columnsMap.put(fieldNames[i], i);
        }

        // init sink column index and partition field index, we will use the column index to found the data in SeaTunnelRow
        this.sinkColumnsIndexInRow = this.sinkColumnList.stream()
            .map(columnsMap::get)
            .collect(Collectors.toList());

        if (!CollectionUtils.isEmpty(this.partitionFieldList)) {
            this.partitionFieldsIndexInRow = this.partitionFieldList.stream()
                .map(columnsMap::get)
                .collect(Collectors.toList());
        }

        if (config.hasPath(Constant.MAX_ROWS_IN_MEMORY)) {
            this.maxRowsInMemory = config.getInt(Constant.MAX_ROWS_IN_MEMORY);
        }
    }
}
