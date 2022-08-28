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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.utils.VariablesSubstitute;
import org.apache.seatunnel.connectors.seatunnel.file.config.Constant;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo2;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.TextFileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState2;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;

import com.google.common.collect.Lists;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public abstract class AbstractWriteStrategy implements WriteStrategy {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final TextFileSinkConfig textFileSinkConfig;
    protected final List<Integer> sinkColumnsIndexInRow;
    protected String jobId;
    protected int subTaskIndex;
    protected HadoopConf hadoopConf;
    protected String transactionId;
    protected String transactionDirectory;
    protected Map<String, String> needMoveFiles;
    protected Map<String, String> beingWrittenFile;
    private Map<String, List<String>> partitionDirAndValuesMap;
    protected SeaTunnelRowType seaTunnelRowType;
    protected Long checkpointId = 1L;

    public AbstractWriteStrategy(TextFileSinkConfig textFileSinkConfig) {
        this.textFileSinkConfig = textFileSinkConfig;
        this.sinkColumnsIndexInRow = textFileSinkConfig.getSinkColumnsIndexInRow();
    }

    /**
     * init hadoop conf
     *
     * @param conf hadoop conf
     */
    @Override
    public void init(HadoopConf conf, String jobId, int subTaskIndex) {
        this.hadoopConf = conf;
        this.jobId = jobId;
        this.subTaskIndex = subTaskIndex;
        this.transactionId = "T" + Constant.TRANSACTION_ID_SPLIT + jobId + Constant.TRANSACTION_ID_SPLIT + subTaskIndex + Constant.TRANSACTION_ID_SPLIT + checkpointId;
        this.transactionDirectory = getTransactionDir(this.transactionId);
        this.needMoveFiles = new HashMap<>();
        this.partitionDirAndValuesMap = new HashMap<>();
        this.beingWrittenFile = new HashMap<>();
    }

    /**
     * use hadoop conf generate hadoop configuration
     *
     * @param conf hadoop conf
     * @return Configuration
     */
    @Override
    public Configuration getConfiguration(HadoopConf conf) {
        Configuration configuration = new Configuration();
        if (hadoopConf != null) {
            configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hadoopConf.getHdfsNameKey());
            configuration.set("fs.hdfs.impl", hadoopConf.getFsHdfsImpl());
            hadoopConf.setExtraOptionsForConfiguration(configuration);
        }
        return configuration;
    }

    /**
     * set seaTunnelRowTypeInfo in writer
     *
     * @param seaTunnelRowType seaTunnelRowType
     */
    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    /**
     * use seaTunnelRow generate partition directory
     *
     * @param seaTunnelRow seaTunnelRow
     * @return the map of partition directory
     */
    @Override
    public Map<String, List<String>> generatorPartitionDir(SeaTunnelRow seaTunnelRow) {
        List<String> partitionFieldList = textFileSinkConfig.getPartitionFieldList();
        List<Integer> partitionFieldsIndexInRow = textFileSinkConfig.getPartitionFieldsIndexInRow();
        String partitionDirExpression = textFileSinkConfig.getPartitionDirExpression();
        String[] keys = new String[partitionFieldList.size()];
        String[] values = new String[partitionFieldList.size()];
        for (int i = 0; i < partitionFieldList.size(); i++) {
            keys[i] = "k" + i;
            values[i] = "v" + i;
        }
        Map<String, List<String>> partitionDirAndValuesMap = new HashMap<>(1);
        if (CollectionUtils.isEmpty(partitionFieldsIndexInRow)) {
            partitionDirAndValuesMap.put(Constant.NON_PARTITION, null);
            return partitionDirAndValuesMap;
        }
        List<String> vals = new ArrayList<>(partitionFieldsIndexInRow.size());
        String partitionDir;
        if (StringUtils.isBlank(partitionDirExpression)) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < partitionFieldsIndexInRow.size(); i++) {
                stringBuilder.append(partitionFieldList.get(i))
                        .append("=")
                        .append(seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)])
                        .append("/");
                vals.add(seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
            }
            partitionDir = stringBuilder.toString();
        } else {
            Map<String, String> valueMap = new HashMap<>(partitionFieldList.size() * 2);
            for (int i = 0; i < partitionFieldsIndexInRow.size(); i++) {
                valueMap.put(keys[i], partitionFieldList.get(i));
                valueMap.put(values[i], seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
                vals.add(seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
            }
            partitionDir = VariablesSubstitute.substitute(partitionDirExpression, valueMap);
        }
        partitionDirAndValuesMap.put(partitionDir, vals);
        return partitionDirAndValuesMap;
    }

    /**
     * use transaction id generate file name
     *
     * @param transactionId transaction id
     * @return file name
     */
    @Override
    public String generateFileName(String transactionId) {
        String fileNameExpression = textFileSinkConfig.getFileNameExpression();
        FileFormat fileFormat = textFileSinkConfig.getFileFormat();
        if (StringUtils.isBlank(fileNameExpression)) {
            return transactionId + fileFormat.getSuffix();
        }
        String timeFormat = textFileSinkConfig.getFileNameTimeFormat();
        DateTimeFormatter df = DateTimeFormatter.ofPattern(timeFormat);
        String formattedDate = df.format(ZonedDateTime.now());
        Map<String, String> valuesMap = new HashMap<>();
        valuesMap.put(Constants.UUID, UUID.randomUUID().toString());
        valuesMap.put(Constants.NOW, formattedDate);
        valuesMap.put(timeFormat, formattedDate);
        valuesMap.put(Constant.TRANSACTION_EXPRESSION, transactionId);
        String substitute = VariablesSubstitute.substitute(fileNameExpression, valuesMap);
        return substitute + fileFormat.getSuffix();
    }

    /**
     * prepare commit operation
     *
     * @return the file commit information
     */
    @Override
    public Optional<FileCommitInfo2> prepareCommit() {
        this.finishAndCloseFile();
        // this.needMoveFiles will be clear when beginTransaction, so we need copy the needMoveFiles.
        Map<String, String> commitMap = new HashMap<>(this.needMoveFiles);
        Map<String, List<String>> copyMap = this.partitionDirAndValuesMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue())));
        return Optional.of(new FileCommitInfo2(commitMap, copyMap, transactionDirectory));
    }

    /**
     * abort prepare commit operation
     */
    @Override
    public void abortPrepare() {
        try {
            FileSystemUtils.deleteFile(transactionDirectory);
        } catch (IOException e) {
            throw new RuntimeException("abort transaction " + this.transactionId + " error.", e);
        }
    }

    /**
     * when a checkpoint was triggered, snapshot the state of connector
     *
     * @param checkpointId checkpointId
     * @return the list of states
     */
    @Override
    public List<FileSinkState2> snapshotState(long checkpointId) {
        ArrayList<FileSinkState2> fileState = Lists.newArrayList(new FileSinkState2(this.transactionId, this.checkpointId));
        this.checkpointId = checkpointId;
        return fileState;
    }

    private String getTransactionDir(@NonNull String transactionId) {
        String[] strings = new String[]{textFileSinkConfig.getTmpPath(), Constant.SEATUNNEL, jobId, transactionId};
        return String.join(File.separator, strings);
    }

    public String getOrCreateFilePathBeingWritten(@NonNull SeaTunnelRow seaTunnelRow) {
        Map<String, List<String>> dataPartitionDirAndValuesMap = generatorPartitionDir(seaTunnelRow);
        String beingWrittenFileKey = dataPartitionDirAndValuesMap.keySet().toArray()[0].toString();
        // get filePath from beingWrittenFile
        String beingWrittenFilePath = beingWrittenFile.get(beingWrittenFileKey);
        if (beingWrittenFilePath != null) {
            return beingWrittenFilePath;
        } else {
            String[] strings = new String[]{transactionDirectory, beingWrittenFileKey, generateFileName(transactionId)};
            String newBeingWrittenFilePath = String.join(File.separator, strings);
            beingWrittenFile.put(beingWrittenFileKey, newBeingWrittenFilePath);
            if (!Constant.NON_PARTITION.equals(dataPartitionDirAndValuesMap.keySet().toArray()[0].toString())){
                partitionDirAndValuesMap.putAll(dataPartitionDirAndValuesMap);
            }
            return newBeingWrittenFilePath;
        }
    }

    public String getTargetLocation(@NonNull String seaTunnelFilePath) {
        String tmpPath = seaTunnelFilePath.replaceAll(transactionDirectory, textFileSinkConfig.getPath());
        return tmpPath.replaceAll(Constant.NON_PARTITION + File.separator, "");
    }
}
