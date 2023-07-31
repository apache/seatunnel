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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.VariablesSubstitute;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;
import static org.apache.parquet.avro.AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_FIXED_AS_INT96;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE;

public abstract class AbstractWriteStrategy implements WriteStrategy {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final FileSinkConfig fileSinkConfig;
    protected final CompressFormat compressFormat;
    protected final List<Integer> sinkColumnsIndexInRow;
    protected String jobId;
    protected int subTaskIndex;
    protected HadoopConf hadoopConf;
    protected FileSystemUtils fileSystemUtils;
    protected String transactionId;
    /** The uuid prefix to make sure same job different file sink will not conflict. */
    protected String uuidPrefix;

    protected String transactionDirectory;
    protected LinkedHashMap<String, String> needMoveFiles;
    protected LinkedHashMap<String, String> beingWrittenFile = new LinkedHashMap<>();
    private LinkedHashMap<String, List<String>> partitionDirAndValuesMap;
    protected SeaTunnelRowType seaTunnelRowType;

    // Checkpoint id from engine is start with 1
    protected Long checkpointId = 0L;
    protected int partId = 0;
    protected int batchSize;
    protected int currentBatchSize = 0;
    protected boolean isKerberosAuthorization = false;

    public AbstractWriteStrategy(FileSinkConfig fileSinkConfig) {
        this.fileSinkConfig = fileSinkConfig;
        this.sinkColumnsIndexInRow = fileSinkConfig.getSinkColumnsIndexInRow();
        this.batchSize = fileSinkConfig.getBatchSize();
        this.compressFormat = fileSinkConfig.getCompressFormat();
    }

    /**
     * init hadoop conf
     *
     * @param conf hadoop conf
     */
    @Override
    public void init(HadoopConf conf, String jobId, String uuidPrefix, int subTaskIndex) {
        this.hadoopConf = conf;
        this.jobId = jobId;
        this.subTaskIndex = subTaskIndex;
        this.uuidPrefix = uuidPrefix;
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) throws FileConnectorException {
        if (currentBatchSize >= batchSize) {
            newFilePart();
            currentBatchSize = 0;
        }
        currentBatchSize++;
    }

    public synchronized void newFilePart() {
        this.partId++;
        beingWrittenFile.clear();
        log.debug("new file part: {}", partId);
    }

    protected SeaTunnelRowType buildSchemaWithRowType(
            SeaTunnelRowType seaTunnelRowType, List<Integer> sinkColumnsIndex) {
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        List<String> newFieldNames = new ArrayList<>();
        List<SeaTunnelDataType<?>> newFieldTypes = new ArrayList<>();
        sinkColumnsIndex.forEach(
                index -> {
                    newFieldNames.add(fieldNames[index]);
                    newFieldTypes.add(fieldTypes[index]);
                });
        return new SeaTunnelRowType(
                newFieldNames.toArray(new String[0]),
                newFieldTypes.toArray(new SeaTunnelDataType[0]));
    }

    /**
     * use hadoop conf generate hadoop configuration
     *
     * @param hadoopConf hadoop conf
     * @return Configuration
     */
    @Override
    public Configuration getConfiguration(HadoopConf hadoopConf) {
        Configuration configuration = new Configuration();
        configuration.setBoolean(READ_INT96_AS_FIXED, true);
        configuration.setBoolean(WRITE_FIXED_AS_INT96, true);
        configuration.setBoolean(ADD_LIST_ELEMENT_RECORDS, false);
        configuration.setBoolean(WRITE_OLD_LIST_STRUCTURE, false);
        configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hadoopConf.getHdfsNameKey());
        configuration.set(
                String.format("fs.%s.impl", hadoopConf.getSchema()), hadoopConf.getFsHdfsImpl());
        this.hadoopConf.setExtraOptionsForConfiguration(configuration);
        String principal = hadoopConf.getKerberosPrincipal();
        String keytabPath = hadoopConf.getKerberosKeytabPath();
        if (!isKerberosAuthorization) {
            FileSystemUtils.doKerberosAuthentication(configuration, principal, keytabPath);
            isKerberosAuthorization = true;
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
    public LinkedHashMap<String, List<String>> generatorPartitionDir(SeaTunnelRow seaTunnelRow) {
        List<Integer> partitionFieldsIndexInRow = fileSinkConfig.getPartitionFieldsIndexInRow();
        LinkedHashMap<String, List<String>> partitionDirAndValuesMap = new LinkedHashMap<>(1);
        if (CollectionUtils.isEmpty(partitionFieldsIndexInRow)) {
            partitionDirAndValuesMap.put(BaseSinkConfig.NON_PARTITION, null);
            return partitionDirAndValuesMap;
        }
        List<String> partitionFieldList = fileSinkConfig.getPartitionFieldList();
        String partitionDirExpression = fileSinkConfig.getPartitionDirExpression();
        String[] keys = new String[partitionFieldList.size()];
        String[] values = new String[partitionFieldList.size()];
        for (int i = 0; i < partitionFieldList.size(); i++) {
            keys[i] = "k" + i;
            values[i] = "v" + i;
        }
        List<String> vals = new ArrayList<>(partitionFieldsIndexInRow.size());
        String partitionDir;
        if (StringUtils.isBlank(partitionDirExpression)) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < partitionFieldsIndexInRow.size(); i++) {
                stringBuilder
                        .append(partitionFieldList.get(i))
                        .append("=")
                        .append(seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)]);
                if (i < partitionFieldsIndexInRow.size() - 1) {
                    stringBuilder.append("/");
                }
                vals.add(seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
            }
            partitionDir = stringBuilder.toString();
        } else {
            Map<String, String> valueMap = new HashMap<>(partitionFieldList.size() * 2);
            for (int i = 0; i < partitionFieldsIndexInRow.size(); i++) {
                valueMap.put(keys[i], partitionFieldList.get(i));
                valueMap.put(
                        values[i],
                        seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
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
        String fileNameExpression = fileSinkConfig.getFileNameExpression();
        FileFormat fileFormat = fileSinkConfig.getFileFormat();
        String suffix = fileFormat.getSuffix();
        suffix = compressFormat.getCompressCodec() + suffix;
        if (StringUtils.isBlank(fileNameExpression)) {
            return transactionId + suffix;
        }
        String timeFormat = fileSinkConfig.getFileNameTimeFormat();
        DateTimeFormatter df = DateTimeFormatter.ofPattern(timeFormat);
        String formattedDate = df.format(ZonedDateTime.now());
        Map<String, String> valuesMap = new HashMap<>();
        valuesMap.put(Constants.UUID, UUID.randomUUID().toString());
        valuesMap.put(Constants.NOW, formattedDate);
        valuesMap.put(timeFormat, formattedDate);
        valuesMap.put(BaseSinkConfig.TRANSACTION_EXPRESSION, transactionId);
        String substitute =
                VariablesSubstitute.substitute(fileNameExpression, valuesMap) + "_" + partId;
        return substitute + suffix;
    }

    /**
     * prepare commit operation
     *
     * @return the file commit information
     */
    @Override
    public Optional<FileCommitInfo> prepareCommit() {
        this.finishAndCloseFile();
        LinkedHashMap<String, String> commitMap = new LinkedHashMap<>(this.needMoveFiles);
        LinkedHashMap<String, List<String>> copyMap =
                this.partitionDirAndValuesMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> new ArrayList<>(e.getValue()),
                                        (e1, e2) -> e1,
                                        LinkedHashMap::new));
        return Optional.of(new FileCommitInfo(commitMap, copyMap, transactionDirectory));
    }

    /** abort prepare commit operation */
    @Override
    public void abortPrepare() {
        abortPrepare(transactionId);
    }

    /**
     * abort prepare commit operation using transaction directory
     *
     * @param transactionId transaction id
     */
    public void abortPrepare(String transactionId) {
        try {
            fileSystemUtils.deleteFile(getTransactionDir(transactionId));
        } catch (IOException e) {
            throw new FileConnectorException(
                    CommonErrorCode.FILE_OPERATION_FAILED,
                    "Abort transaction "
                            + transactionId
                            + " error, delete transaction directory failed",
                    e);
        }
    }

    /**
     * when a checkpoint completed, file connector should begin a new transaction and generate new
     * transaction id
     *
     * @param checkpointId checkpoint id
     */
    public void beginTransaction(Long checkpointId) {
        this.checkpointId = checkpointId;
        this.transactionId = getTransactionId(checkpointId);
        this.transactionDirectory = getTransactionDir(this.transactionId);
        this.needMoveFiles = new LinkedHashMap<>();
        this.partitionDirAndValuesMap = new LinkedHashMap<>();
    }

    private String getTransactionId(Long checkpointId) {
        return "T"
                + BaseSinkConfig.TRANSACTION_ID_SPLIT
                + jobId
                + BaseSinkConfig.TRANSACTION_ID_SPLIT
                + uuidPrefix
                + BaseSinkConfig.TRANSACTION_ID_SPLIT
                + subTaskIndex
                + BaseSinkConfig.TRANSACTION_ID_SPLIT
                + checkpointId;
    }

    /**
     * when a checkpoint was triggered, snapshot the state of connector
     *
     * @param checkpointId checkpointId
     * @return the list of states
     */
    @Override
    public List<FileSinkState> snapshotState(long checkpointId) {
        LinkedHashMap<String, List<String>> commitMap =
                this.partitionDirAndValuesMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> new ArrayList<>(e.getValue()),
                                        (e1, e2) -> e1,
                                        LinkedHashMap::new));
        ArrayList<FileSinkState> fileState =
                Lists.newArrayList(
                        new FileSinkState(
                                this.transactionId,
                                this.uuidPrefix,
                                this.checkpointId,
                                new LinkedHashMap<>(this.needMoveFiles),
                                commitMap,
                                this.getTransactionDir(transactionId)));
        this.beingWrittenFile.clear();
        this.beginTransaction(checkpointId + 1);
        return fileState;
    }

    /**
     * using transaction id generate transaction directory
     *
     * @param transactionId transaction id
     * @return transaction directory
     */
    private String getTransactionDir(@NonNull String transactionId) {
        String transactionDirectoryPrefix =
                getTransactionDirPrefix(fileSinkConfig.getTmpPath(), jobId, uuidPrefix);
        return String.join(
                File.separator, new String[] {transactionDirectoryPrefix, transactionId});
    }

    public static String getTransactionDirPrefix(String tmpPath, String jobId, String uuidPrefix) {
        String[] strings = new String[] {tmpPath, BaseSinkConfig.SEATUNNEL, jobId, uuidPrefix};
        return String.join(File.separator, strings);
    }

    public String getOrCreateFilePathBeingWritten(@NonNull SeaTunnelRow seaTunnelRow) {
        LinkedHashMap<String, List<String>> dataPartitionDirAndValuesMap =
                generatorPartitionDir(seaTunnelRow);
        String beingWrittenFileKey = dataPartitionDirAndValuesMap.keySet().toArray()[0].toString();
        // get filePath from beingWrittenFile
        String beingWrittenFilePath = beingWrittenFile.get(beingWrittenFileKey);
        if (beingWrittenFilePath != null) {
            return beingWrittenFilePath;
        } else {
            String[] pathSegments =
                    new String[] {
                        transactionDirectory, beingWrittenFileKey, generateFileName(transactionId)
                    };
            String newBeingWrittenFilePath = String.join(File.separator, pathSegments);
            beingWrittenFile.put(beingWrittenFileKey, newBeingWrittenFilePath);
            if (!BaseSinkConfig.NON_PARTITION.equals(
                    dataPartitionDirAndValuesMap.keySet().toArray()[0].toString())) {
                partitionDirAndValuesMap.putAll(dataPartitionDirAndValuesMap);
            }
            return newBeingWrittenFilePath;
        }
    }

    public String getTargetLocation(@NonNull String seaTunnelFilePath) {
        String tmpPath =
                seaTunnelFilePath.replaceAll(
                        Matcher.quoteReplacement(transactionDirectory),
                        Matcher.quoteReplacement(fileSinkConfig.getPath()));
        return tmpPath.replaceAll(
                BaseSinkConfig.NON_PARTITION + Matcher.quoteReplacement(File.separator), "");
    }

    @Override
    public long getCheckpointId() {
        return this.checkpointId;
    }

    @Override
    public FileSinkConfig getFileSinkConfig() {
        return fileSinkConfig;
    }

    @Override
    public FileSystemUtils getFileSystemUtils() {
        return fileSystemUtils;
    }

    @Override
    public void setFileSystemUtils(FileSystemUtils fileSystemUtils) {
        this.fileSystemUtils = fileSystemUtils;
    }
}
