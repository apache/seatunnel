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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueType;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.file.config.ArchiveCompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileCompareMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileInfo;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSyncMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileUpdateStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Slf4j
public abstract class AbstractReadStrategy implements ReadStrategy {
    protected static final String[] TYPE_ARRAY_STRING = new String[0];
    protected static final Boolean[] TYPE_ARRAY_BOOLEAN = new Boolean[0];
    protected static final Byte[] TYPE_ARRAY_BYTE = new Byte[0];
    protected static final Short[] TYPE_ARRAY_SHORT = new Short[0];
    protected static final Integer[] TYPE_ARRAY_INTEGER = new Integer[0];
    protected static final Long[] TYPE_ARRAY_LONG = new Long[0];
    protected static final Float[] TYPE_ARRAY_FLOAT = new Float[0];
    protected static final Double[] TYPE_ARRAY_DOUBLE = new Double[0];
    protected static final BigDecimal[] TYPE_ARRAY_BIG_DECIMAL = new BigDecimal[0];
    protected static final LocalDate[] TYPE_ARRAY_LOCAL_DATE = new LocalDate[0];
    protected static final LocalDateTime[] TYPE_ARRAY_LOCAL_DATETIME = new LocalDateTime[0];

    protected HadoopConf hadoopConf;
    protected SeaTunnelRowType seaTunnelRowType;
    protected SeaTunnelRowType seaTunnelRowTypeWithPartition;
    protected Config pluginConfig;
    protected ReadonlyConfig readonlyConfig;
    protected List<String> fileNames = new ArrayList<>();
    protected List<String> readPartitions = new ArrayList<>();
    protected List<String> readColumns = new ArrayList<>();
    protected boolean isMergePartition = true;
    protected long skipHeaderNumber = FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER.defaultValue();
    protected transient boolean isKerberosAuthorization = false;
    protected String filenameExtension;
    protected HadoopFileSystemProxy hadoopFileSystemProxy;
    protected ArchiveCompressFormat archiveCompressFormat =
            FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC.defaultValue();

    protected Pattern pattern;
    protected Date fileModifiedStartDate;
    protected Date fileModifiedEndDate;
    protected String fileBasePath;

    protected boolean enableSplitFile;

    protected String sourceRootPath;
    protected boolean enableUpdateSync;
    protected String targetPath;
    protected FileUpdateStrategy updateStrategy =
            FileBaseSourceOptions.UPDATE_STRATEGY.defaultValue();
    protected FileCompareMode compareMode = FileBaseSourceOptions.COMPARE_MODE.defaultValue();
    protected Map<String, String> targetHadoopConf;
    protected transient HadoopFileSystemProxy targetHadoopFileSystemProxy;
    protected transient boolean shareTargetFileSystemProxy;
    protected transient boolean checksumUnavailableWarned;
    protected boolean sortFilesByModTime =
            FileBaseSourceOptions.SORT_FILES_BY_MOD_TIME.defaultValue();

    private static final class UpdateModeStats {
        private long scanned;
        private long skipped;
    }

    @Override
    public void init(HadoopConf conf) {
        this.hadoopConf = conf;
        this.hadoopFileSystemProxy = new HadoopFileSystemProxy(hadoopConf);
        if (enableUpdateSync) {
            initTargetHadoopFileSystemProxy();
        }
    }

    @Override
    public void setCatalogTable(CatalogTable catalogTable) {
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        this.seaTunnelRowTypeWithPartition =
                mergePartitionTypes(getPathForPartitionInference(null), this.seaTunnelRowType);
    }

    boolean checkFileType(String path) {
        return true;
    }

    @Override
    public List<String> getFileNamesByPath(String path) throws IOException {
        List<FileInfo> fileInfoList = new ArrayList<>();
        UpdateModeStats updateModeStats = enableUpdateSync ? new UpdateModeStats() : null;
        collectFileInfoByPath(path, fileInfoList, updateModeStats);

        // Sort by modification time in descending order if enabled
        if (sortFilesByModTime) {
            fileInfoList.sort(Comparator.comparingLong(FileInfo::getModifyTime).reversed());
        }

        if (updateModeStats != null) {
            log.info(
                    "Update sync mode statistics: scanned={}, skipped={}, to_sync={}",
                    updateModeStats.scanned,
                    updateModeStats.skipped,
                    updateModeStats.scanned - updateModeStats.skipped);
        }

        for (FileInfo fileInfo : fileInfoList) {
            this.fileNames.add(fileInfo.getFileName());
        }

        return fileInfoList.stream().map(FileInfo::getFileName).collect(Collectors.toList());
    }

    private void collectFileInfoByPath(
            String path, List<FileInfo> fileInfoList, UpdateModeStats updateModeStats)
            throws IOException {
        FileStatus[] stats = hadoopFileSystemProxy.listStatus(path);
        for (FileStatus fileStatus : stats) {
            if (fileStatus.isDirectory()) {
                // skip hidden tmp directory, such as .hive-staging_hive
                if (!fileStatus.getPath().getName().startsWith(".")) {
                    collectFileInfoByPath(
                            fileStatus.getPath().toString(), fileInfoList, updateModeStats);
                }
                continue;
            }
            if (!fileStatus.isFile()
                    || !filterFileByPattern(fileStatus)
                    || fileStatus.getLen() <= 0) {
                continue;
            }

            // filter '_SUCCESS' file and hidden files
            String fileName = fileStatus.getPath().getName();
            if (fileName.equals("_SUCCESS")
                    || fileName.startsWith(".")
                    || !filterFileByModificationDate(fileStatus)) {
                continue;
            }

            String filePath = fileStatus.getPath().toString();
            if (StringUtils.isNotEmpty(filenameExtension)
                    && !filePath.endsWith(filenameExtension)) {
                continue;
            }

            if (!readPartitions.isEmpty()) {
                boolean partitionMatched = false;
                for (String readPartition : readPartitions) {
                    if (filePath.contains(readPartition)) {
                        partitionMatched = true;
                        break;
                    }
                }
                if (!partitionMatched) {
                    continue;
                }
            }

            if (updateModeStats != null) {
                updateModeStats.scanned++;
            }
            if (shouldSyncFileInUpdateMode(fileStatus)) {
                FileInfo fileInfo = new FileInfo(filePath, fileStatus.getModificationTime());
                fileInfoList.add(fileInfo);
            } else if (updateModeStats != null) {
                updateModeStats.skipped++;
            }
        }
    }

    private Date getFileModifiedDate(String modifiedDate) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (modifiedDate != null) {
            try {
                return dateFormat.parse(modifiedDate);
            } catch (ParseException e) {
                throw new IllegalArgumentException(
                        "Failed to parse file modified date format: yyyy-MM-dd HH:mm:ss, please check file_filter_modified_start or file_filter_modified_end format.");
            }
        }

        return null;
    }

    protected boolean filterFileByModificationDate(FileStatus fileStatus) {

        long fileModifiedTime = fileStatus.getModificationTime();

        // Both start and end date are set
        if (fileModifiedStartDate != null && fileModifiedEndDate != null) {
            return fileModifiedTime >= fileModifiedStartDate.getTime()
                    && fileModifiedTime < fileModifiedEndDate.getTime();
        }

        // Only start date is set
        if (fileModifiedStartDate != null) {
            return fileModifiedTime >= fileModifiedStartDate.getTime();
        }

        // Only end date is set
        if (fileModifiedEndDate != null) {
            return fileModifiedTime < fileModifiedEndDate.getTime();
        }

        // Neither start nor end date is set
        return true;
    }

    @Override
    public void setPluginConfig(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
        this.readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        // Determine whether it is a compressed file
        if (pluginConfig.hasPath(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC.key())) {
            String archiveCompressCodec =
                    pluginConfig.getString(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC.key());
            archiveCompressFormat =
                    ArchiveCompressFormat.valueOf(archiveCompressCodec.toUpperCase());
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.PARSE_PARTITION_FROM_PATH.key())) {
            isMergePartition =
                    pluginConfig.getBoolean(FileBaseSourceOptions.PARSE_PARTITION_FROM_PATH.key());
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER.key())) {
            skipHeaderNumber =
                    pluginConfig.getLong(FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER.key());
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.FILENAME_EXTENSION.key())) {
            filenameExtension =
                    pluginConfig.getString(FileBaseSourceOptions.FILENAME_EXTENSION.key());
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.READ_PARTITIONS.key())) {
            readPartitions.addAll(
                    pluginConfig.getStringList(FileBaseSourceOptions.READ_PARTITIONS.key()));
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.READ_COLUMNS.key())) {
            readColumns.addAll(
                    pluginConfig.getStringList(FileBaseSourceOptions.READ_COLUMNS.key()));
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.FILE_FILTER_PATTERN.key())) {
            String filterPattern =
                    pluginConfig.getString(FileBaseSourceOptions.FILE_FILTER_PATTERN.key());
            this.pattern = Pattern.compile(filterPattern);
            // because 'ConfigFactory.systemProperties()' has a 'path' parameter, it is necessary to
            // obtain 'path' under the premise of 'FILE_FILTER_PATTERN'
            if (pluginConfig.hasPath(FileBaseSourceOptions.FILE_PATH.key())
                    && pluginConfig.getValue(FileBaseSourceOptions.FILE_PATH.key()).valueType()
                            == ConfigValueType.STRING) {
                fileBasePath = pluginConfig.getString(FileBaseSourceOptions.FILE_PATH.key());
            }
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.FILE_FILTER_MODIFIED_START.key())) {
            fileModifiedStartDate =
                    getFileModifiedDate(
                            pluginConfig.getString(
                                    FileBaseSourceOptions.FILE_FILTER_MODIFIED_START.key()));
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.FILE_FILTER_MODIFIED_END.key())) {
            fileModifiedEndDate =
                    getFileModifiedDate(
                            pluginConfig.getString(
                                    FileBaseSourceOptions.FILE_FILTER_MODIFIED_END.key()));
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key())) {
            enableSplitFile =
                    pluginConfig.getBoolean(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key());
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.SORT_FILES_BY_MOD_TIME.key())) {
            sortFilesByModTime =
                    pluginConfig.getBoolean(FileBaseSourceOptions.SORT_FILES_BY_MOD_TIME.key());
        }

        if (pluginConfig.hasPath(FileBaseSourceOptions.FILE_PATH.key())
                && pluginConfig.getValue(FileBaseSourceOptions.FILE_PATH.key()).valueType()
                        == ConfigValueType.STRING) {
            sourceRootPath = pluginConfig.getString(FileBaseSourceOptions.FILE_PATH.key());
        }

        FileSyncMode syncMode = FileBaseSourceOptions.SYNC_MODE.defaultValue();
        if (pluginConfig.hasPath(FileBaseSourceOptions.SYNC_MODE.key())) {
            syncMode =
                    parseEnumValue(
                            FileSyncMode.class,
                            pluginConfig.getString(FileBaseSourceOptions.SYNC_MODE.key()),
                            FileBaseSourceOptions.SYNC_MODE.key());
        }
        enableUpdateSync = syncMode == FileSyncMode.UPDATE;
        if (enableUpdateSync) {
            validateUpdateSyncConfig(pluginConfig);
            log.info(
                    "Update sync mode enabled: source_path={}, target_path={}, update_strategy={}, compare_mode={}",
                    maskUriUserInfo(sourceRootPath),
                    maskUriUserInfo(targetPath),
                    updateStrategy.name().toLowerCase(Locale.ROOT),
                    compareMode.name().toLowerCase(Locale.ROOT));
        }
    }

    @Override
    public SeaTunnelRowType getActualSeaTunnelRowTypeInfo() {
        return isMergePartition ? seaTunnelRowTypeWithPartition : seaTunnelRowType;
    }

    protected void resolveArchiveCompressedInputStream(
            FileSourceSplit split,
            Collector<SeaTunnelRow> output,
            Map<String, String> partitionsMap,
            FileFormat fileFormat)
            throws IOException {
        String path = split.getFilePath();
        String tableId = split.getTableId();
        switch (archiveCompressFormat) {
            case ZIP:
                try (ZipInputStream zis =
                        new ZipInputStream(hadoopFileSystemProxy.getInputStream(path))) {
                    ZipEntry entry;
                    while ((entry = zis.getNextEntry()) != null) {
                        if (!entry.isDirectory() && checkFileType(entry.getName(), fileFormat)) {
                            readProcess(
                                    split,
                                    output,
                                    copyInputStream(zis),
                                    partitionsMap,
                                    entry.getName());
                        }
                        zis.closeEntry();
                    }
                }
                break;
            case TAR:
                try (TarArchiveInputStream tarInput =
                        new TarArchiveInputStream(hadoopFileSystemProxy.getInputStream(path))) {
                    TarArchiveEntry entry;
                    while ((entry = tarInput.getNextTarEntry()) != null) {
                        if (!entry.isDirectory() && checkFileType(entry.getName(), fileFormat)) {
                            readProcess(
                                    split,
                                    output,
                                    copyInputStream(tarInput),
                                    partitionsMap,
                                    entry.getName());
                        }
                    }
                }
                break;
            case TAR_GZ:
                try (GzipCompressorInputStream gzipIn =
                                new GzipCompressorInputStream(
                                        hadoopFileSystemProxy.getInputStream(path));
                        TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)) {

                    TarArchiveEntry entry;
                    while ((entry = tarIn.getNextTarEntry()) != null) {
                        if (!entry.isDirectory() && checkFileType(entry.getName(), fileFormat)) {
                            readProcess(
                                    split,
                                    output,
                                    copyInputStream(tarIn),
                                    partitionsMap,
                                    entry.getName());
                        }
                    }
                }
                break;
            case GZ:
                GzipCompressorInputStream gzipIn =
                        new GzipCompressorInputStream(hadoopFileSystemProxy.getInputStream(path));
                GzipParameters parameters = gzipIn.getMetaData();
                String fileName = parameters.getFilename();
                if (fileName == null) {
                    // remove file suffix
                    // eg: excel need full compressed name
                    if (fileFormat == FileFormat.EXCEL) {
                        if (path.endsWith(".gz")) {
                            fileName = path.substring(0, path.length() - 3);
                        } else {
                            throw new IllegalArgumentException(
                                    "Excel file must have a .gz extension. File: " + path);
                        }
                    } else {
                        fileName = path;
                    }
                }
                readProcess(split, output, copyInputStream(gzipIn), partitionsMap, fileName);
                break;
            case NONE:
                readProcess(
                        split,
                        output,
                        hadoopFileSystemProxy.getInputStream(path),
                        partitionsMap,
                        path);
                break;
            default:
                log.warn(
                        "The file does not support this archive compress type: {}",
                        archiveCompressFormat);
                readProcess(
                        split,
                        output,
                        hadoopFileSystemProxy.getInputStream(path),
                        partitionsMap,
                        path);
        }
    }

    protected void readProcess(
            FileSourceSplit split,
            Collector<SeaTunnelRow> output,
            InputStream inputStream,
            Map<String, String> partitionsMap,
            String currentFileName)
            throws IOException {
        throw new UnsupportedOperationException(
                "The file does not support the compressed file reading");
    }

    protected Map<String, String> parsePartitionsByPath(String path) {
        LinkedHashMap<String, String> partitions = new LinkedHashMap<>();
        if (StringUtils.isBlank(path)) {
            return partitions;
        }
        Arrays.stream(path.split("/", -1))
                .filter(split -> split.contains("="))
                .map(split -> split.split("=", -1))
                .forEach(kv -> partitions.put(kv[0], kv[1]));
        return partitions;
    }

    protected String getPathForPartitionInference(String fallbackPath) {
        if (!fileNames.isEmpty()) {
            return fileNames.get(0);
        }
        if (StringUtils.isNotBlank(fallbackPath)) {
            return fallbackPath;
        }
        return sourceRootPath;
    }

    protected SeaTunnelRowType mergePartitionTypes(String path, SeaTunnelRowType seaTunnelRowType) {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        if (partitionsMap.isEmpty()) {
            return seaTunnelRowType;
        }
        // get all names of partitions fields
        String[] partitionNames = partitionsMap.keySet().toArray(TYPE_ARRAY_STRING);
        // initialize data type for partition fields
        SeaTunnelDataType<?>[] partitionTypes = new SeaTunnelDataType<?>[partitionNames.length];
        Arrays.fill(partitionTypes, BasicType.STRING_TYPE);
        // get origin field names
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        // get origin data types
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        // create new array to merge partition fields and origin fields
        String[] newFieldNames = new String[fieldNames.length + partitionNames.length];
        // create new array to merge partition fields' data type and origin fields' data type
        SeaTunnelDataType<?>[] newFieldTypes =
                new SeaTunnelDataType<?>[fieldTypes.length + partitionTypes.length];
        // copy origin field names to new array
        System.arraycopy(fieldNames, 0, newFieldNames, 0, fieldNames.length);
        // copy partitions field name to new array
        System.arraycopy(
                partitionNames, 0, newFieldNames, fieldNames.length, partitionNames.length);
        // copy origin field types to new array
        System.arraycopy(fieldTypes, 0, newFieldTypes, 0, fieldTypes.length);
        // copy partition field types to new array
        System.arraycopy(
                partitionTypes, 0, newFieldTypes, fieldTypes.length, partitionTypes.length);
        // return merge row type
        return new SeaTunnelRowType(newFieldNames, newFieldTypes);
    }

    protected boolean filterFileByPattern(FileStatus fileStatus) {
        if (Objects.nonNull(pattern) && Objects.nonNull(fileBasePath)) {
            if (pattern.pattern().startsWith(fileBasePath)) {
                // filter based on the file directory at the same time
                String absPath = fileStatus.getPath().toUri().getPath();
                // absPath.substring(absPath.indexOf(fileBasePath), It is to be compatible with
                // scenarios where fileBasePath is a relative path
                return pattern.matcher(absPath.substring(absPath.indexOf(fileBasePath))).matches();
            }
            // filter based on file names
            return pattern.matcher(fileStatus.getPath().getName()).matches();
        }
        return true;
    }

    protected static InputStream copyInputStream(InputStream inputStream) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int bytesRead;

        while ((bytesRead = inputStream.read(buffer)) != -1) {
            byteArrayOutputStream.write(buffer, 0, bytesRead);
        }

        return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    }

    protected boolean checkFileType(String fileName, FileFormat fileFormat) {
        for (String suffix : fileFormat.getAllSuffix()) {
            if (fileName.endsWith(suffix)) {
                return true;
            }
        }

        log.warn(
                "The {} file format is incorrect. Please check the format in the compressed file.",
                fileName);
        return false;
    }

    protected static InputStream safeSlice(InputStream in, long start, long length)
            throws IOException {
        if (start > 0) {
            if (in instanceof Seekable) {
                ((Seekable) in).seek(start);
            } else {
                long toSkip = start;
                while (toSkip > 0) {
                    long skipped = in.skip(toSkip);
                    if (skipped <= 0) {
                        throw new SeaTunnelException("skipped error");
                    }
                    toSkip -= skipped;
                }
            }
        }
        if (length < 0) {
            return in;
        }
        return new BoundedInputStream(in, length);
    }

    protected static BufferedReader createBomAwareBufferedReader(
            InputStream inputStream, String encoding) throws IOException {
        BOMInputStream bomInputStream =
                new BOMInputStream(
                        inputStream,
                        ByteOrderMark.UTF_8,
                        ByteOrderMark.UTF_16BE,
                        ByteOrderMark.UTF_16LE,
                        ByteOrderMark.UTF_32BE,
                        ByteOrderMark.UTF_32LE);
        ByteOrderMark bom = bomInputStream.getBOM();
        Charset charset =
                bom == null ? Charset.forName(encoding) : Charset.forName(bom.getCharsetName());
        return new BufferedReader(new InputStreamReader(bomInputStream, charset));
    }

    @Override
    public void close() throws IOException {
        try {
            if (targetHadoopFileSystemProxy != null && !shareTargetFileSystemProxy) {
                targetHadoopFileSystemProxy.close();
            }
            if (hadoopFileSystemProxy != null) {
                hadoopFileSystemProxy.close();
            }
        } catch (Exception ignore) {
        }
    }

    private void validateUpdateSyncConfig(Config pluginConfig) {
        if (!pluginConfig.hasPath(FileBaseSourceOptions.FILE_FORMAT_TYPE.key())) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "When sync_mode=update, file_format_type must be set.");
        }
        FileFormat fileFormat =
                FileFormat.valueOf(
                        pluginConfig
                                .getString(FileBaseSourceOptions.FILE_FORMAT_TYPE.key())
                                .toUpperCase());
        if (fileFormat != FileFormat.BINARY) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "sync_mode=update currently only supports file_format_type=binary.");
        }

        if (!pluginConfig.hasPath(FileBaseSourceOptions.TARGET_PATH.key())
                || StringUtils.isBlank(
                        pluginConfig.getString(FileBaseSourceOptions.TARGET_PATH.key()))) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "When sync_mode=update, target_path must be set.");
        }
        targetPath = pluginConfig.getString(FileBaseSourceOptions.TARGET_PATH.key()).trim();

        updateStrategy = FileBaseSourceOptions.UPDATE_STRATEGY.defaultValue();
        if (pluginConfig.hasPath(FileBaseSourceOptions.UPDATE_STRATEGY.key())) {
            updateStrategy =
                    parseEnumValue(
                            FileUpdateStrategy.class,
                            pluginConfig.getString(FileBaseSourceOptions.UPDATE_STRATEGY.key()),
                            FileBaseSourceOptions.UPDATE_STRATEGY.key());
        }

        compareMode = FileBaseSourceOptions.COMPARE_MODE.defaultValue();
        if (pluginConfig.hasPath(FileBaseSourceOptions.COMPARE_MODE.key())) {
            compareMode =
                    parseEnumValue(
                            FileCompareMode.class,
                            pluginConfig.getString(FileBaseSourceOptions.COMPARE_MODE.key()),
                            FileBaseSourceOptions.COMPARE_MODE.key());
        }
        if (updateStrategy == FileUpdateStrategy.DISTCP
                && compareMode != FileCompareMode.LEN_MTIME) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "compare_mode="
                            + compareMode.name().toLowerCase(Locale.ROOT)
                            + " is not supported when update_strategy=distcp.");
        }

        if (pluginConfig.hasPath(FileBaseSourceOptions.TARGET_HADOOP_CONF.key())) {
            ConfigObject configObject =
                    pluginConfig.getObject(FileBaseSourceOptions.TARGET_HADOOP_CONF.key());
            Map<String, Object> raw = configObject.unwrapped();
            Map<String, String> conf = new LinkedHashMap<>(raw.size());
            raw.forEach((k, v) -> conf.put(k, v == null ? null : String.valueOf(v)));
            targetHadoopConf = conf;
        }
    }

    private void initTargetHadoopFileSystemProxy() {
        HadoopConf targetConf = buildTargetHadoopConf();
        if (targetConf == this.hadoopConf) {
            targetHadoopFileSystemProxy = this.hadoopFileSystemProxy;
            shareTargetFileSystemProxy = true;
        } else {
            targetHadoopFileSystemProxy = new HadoopFileSystemProxy(targetConf);
            shareTargetFileSystemProxy = false;
        }
    }

    private HadoopConf buildTargetHadoopConf() {
        if (!enableUpdateSync) {
            return this.hadoopConf;
        }
        Map<String, String> extraOptions =
                targetHadoopConf == null
                        ? new LinkedHashMap<>()
                        : new LinkedHashMap<>(targetHadoopConf);

        String fsDefaultNameKey = hadoopConf.getFsDefaultNameKey();
        String targetDefaultFs = extraOptions.remove(fsDefaultNameKey);

        if (StringUtils.isBlank(targetDefaultFs)) {
            targetDefaultFs = tryDeriveDefaultFsFromPath(targetPath);
        }
        if (StringUtils.isBlank(targetDefaultFs)) {
            targetDefaultFs = hadoopConf.getHdfsNameKey();
        }

        boolean needNewConf =
                !extraOptions.isEmpty()
                        || !Objects.equals(targetDefaultFs, hadoopConf.getHdfsNameKey());
        if (!needNewConf) {
            return this.hadoopConf;
        }

        HadoopConf conf = new HadoopConf(targetDefaultFs);
        conf.setHdfsSitePath(hadoopConf.getHdfsSitePath());
        conf.setRemoteUser(hadoopConf.getRemoteUser());
        conf.setKrb5Path(hadoopConf.getKrb5Path());
        conf.setKerberosPrincipal(hadoopConf.getKerberosPrincipal());
        conf.setKerberosKeytabPath(hadoopConf.getKerberosKeytabPath());
        conf.setExtraOptions(extraOptions);
        return conf;
    }

    private static String tryDeriveDefaultFsFromPath(String basePath) {
        if (StringUtils.isBlank(basePath)) {
            return null;
        }
        try {
            Path path = new Path(basePath);
            if (path.toUri().getScheme() == null) {
                return null;
            }
            if (path.toUri().getAuthority() == null) {
                return null;
            }
            return path.toUri().getScheme() + "://" + path.toUri().getAuthority();
        } catch (Exception e) {
            return null;
        }
    }

    private boolean shouldSyncFileInUpdateMode(FileStatus sourceFileStatus) throws IOException {
        if (!enableUpdateSync) {
            return true;
        }
        if (targetHadoopFileSystemProxy == null) {
            initTargetHadoopFileSystemProxy();
        }
        String sourceFilePath = sourceFileStatus.getPath().toString();
        String relativePath = resolveRelativePath(sourceRootPath, sourceFilePath);
        String targetFilePath = buildTargetFilePath(targetPath, relativePath);

        FileStatus targetFileStatus;
        try {
            targetFileStatus = targetHadoopFileSystemProxy.getFileStatus(targetFilePath);
        } catch (FileNotFoundException e) {
            return true;
        }

        long sourceLen = sourceFileStatus.getLen();
        long targetLen = targetFileStatus.getLen();
        if (sourceLen != targetLen) {
            return true;
        }

        long sourceMtime = sourceFileStatus.getModificationTime();
        long targetMtime = targetFileStatus.getModificationTime();

        if (updateStrategy == FileUpdateStrategy.DISTCP) {
            if (sourceMtime > targetMtime) {
                return true;
            }
            logUpdateModeSkip(sourceFilePath, targetFilePath, "distcp: target newer or same");
            return false;
        }

        if (updateStrategy == FileUpdateStrategy.STRICT) {
            if (compareMode == FileCompareMode.LEN_MTIME) {
                if (sourceMtime != targetMtime) {
                    return true;
                }
                logUpdateModeSkip(
                        sourceFilePath, targetFilePath, "strict len_mtime: len and mtime equal");
                return false;
            }
            if (compareMode == FileCompareMode.CHECKSUM) {
                FileChecksum sourceChecksum = null;
                FileChecksum targetChecksum = null;
                Exception checksumException = null;
                try {
                    sourceChecksum = hadoopFileSystemProxy.getFileChecksum(sourceFilePath);
                    targetChecksum = targetHadoopFileSystemProxy.getFileChecksum(targetFilePath);
                } catch (Exception e) {
                    checksumException = e;
                }

                if (checksumException != null || sourceChecksum == null || targetChecksum == null) {
                    if (!checksumUnavailableWarned) {
                        if (checksumException == null) {
                            log.warn(
                                    "File checksum is not available, fallback to content comparison. source={}, target={}",
                                    maskUriUserInfo(sourceFilePath),
                                    maskUriUserInfo(targetFilePath));
                        } else {
                            log.warn(
                                    "File checksum is not available, fallback to content comparison. source={}, target={}",
                                    maskUriUserInfo(sourceFilePath),
                                    maskUriUserInfo(targetFilePath),
                                    checksumException);
                        }
                        checksumUnavailableWarned = true;
                    }
                    try {
                        boolean sameContent = fileContentEquals(sourceFilePath, targetFilePath);
                        if (sameContent) {
                            logUpdateModeSkip(
                                    sourceFilePath,
                                    targetFilePath,
                                    "strict checksum: content equal (checksum unavailable)");
                        }
                        return !sameContent;
                    } catch (Exception e) {
                        log.warn(
                                "Fallback content comparison failed, fallback to COPY. source={}, target={}",
                                maskUriUserInfo(sourceFilePath),
                                maskUriUserInfo(targetFilePath),
                                e);
                        return true;
                    }
                }
                if (checksumEquals(sourceChecksum, targetChecksum)) {
                    logUpdateModeSkip(
                            sourceFilePath, targetFilePath, "strict checksum: checksum equal");
                    return false;
                }
                return true;
            }
        }

        return true;
    }

    private static boolean checksumEquals(FileChecksum source, FileChecksum target) {
        if (source == null || target == null) {
            return false;
        }
        return Objects.equals(source.getAlgorithmName(), target.getAlgorithmName())
                && source.getLength() == target.getLength()
                && Arrays.equals(source.getBytes(), target.getBytes());
    }

    private boolean fileContentEquals(String sourceFilePath, String targetFilePath)
            throws IOException {
        try (InputStream sourceIn = hadoopFileSystemProxy.getInputStream(sourceFilePath);
                InputStream targetIn = targetHadoopFileSystemProxy.getInputStream(targetFilePath)) {
            byte[] sourceBuffer = new byte[8 * 1024];
            byte[] targetBuffer = new byte[8 * 1024];

            while (true) {
                int sourceRead = sourceIn.read(sourceBuffer);
                int targetRead = targetIn.read(targetBuffer);
                if (sourceRead != targetRead) {
                    return false;
                }
                if (sourceRead == -1) {
                    return true;
                }
                for (int i = 0; i < sourceRead; i++) {
                    if (sourceBuffer[i] != targetBuffer[i]) {
                        return false;
                    }
                }
            }
        }
    }

    private static String buildTargetFilePath(String targetBasePath, String relativePath) {
        String cleanRelativePath =
                StringUtils.isBlank(relativePath)
                        ? ""
                        : (relativePath.startsWith("/") ? relativePath.substring(1) : relativePath);
        return new Path(targetBasePath, cleanRelativePath).toString();
    }

    /**
     * Resolve relative path from {@code basePath} to {@code fullFilePath}.
     *
     * <p><b>NOTE:</b> This method is intended for internal use by specific read strategies (for
     * example {@link BinaryReadStrategy}) that need custom path resolution logic.
     *
     * @param basePath base directory path
     * @param fullFilePath full file path
     * @return relative path from base to file
     */
    protected static String resolveRelativePath(String basePath, String fullFilePath) {
        String base = normalizePathPart(basePath);
        String file = normalizePathPart(fullFilePath);
        if (StringUtils.isBlank(file)) {
            return "";
        }
        if (StringUtils.isBlank(base)) {
            return new Path(file).getName();
        }
        if (Objects.equals(base, file)) {
            return new Path(file).getName();
        }
        String basePrefix = base.endsWith("/") ? base : base + "/";
        if (file.startsWith(basePrefix)) {
            return file.substring(basePrefix.length());
        }
        int idx = file.indexOf(basePrefix);
        if (idx >= 0) {
            return file.substring(idx + basePrefix.length());
        }
        return new Path(file).getName();
    }

    private static String normalizePathPart(String path) {
        if (StringUtils.isBlank(path)) {
            return path;
        }
        try {
            return new Path(path).toUri().getPath();
        } catch (Exception e) {
            return path;
        }
    }

    private static String maskUriUserInfo(String rawPath) {
        if (StringUtils.isBlank(rawPath)) {
            return rawPath;
        }
        try {
            java.net.URI uri = new Path(rawPath).toUri();
            if (uri.getUserInfo() == null || uri.getAuthority() == null) {
                return rawPath;
            }
            String maskedAuthority = uri.getAuthority().replace(uri.getUserInfo() + "@", "***@");
            return uri.getScheme()
                    + "://"
                    + maskedAuthority
                    + (uri.getPath() == null ? "" : uri.getPath());
        } catch (Exception e) {
            return rawPath;
        }
    }

    private void logUpdateModeSkip(String sourceFilePath, String targetFilePath, String reason) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Update sync mode skipped file: source={}, target={}, reason={}",
                    maskUriUserInfo(sourceFilePath),
                    maskUriUserInfo(targetFilePath),
                    reason);
        }
    }

    private static <E extends Enum<E>> E parseEnumValue(
            Class<E> enumClass, String rawValue, String optionKey) {
        if (StringUtils.isBlank(rawValue)) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "Option '" + optionKey + "' must not be blank.");
        }
        String normalized = rawValue.trim().toUpperCase(Locale.ROOT);
        for (E v : enumClass.getEnumConstants()) {
            if (v.name().equalsIgnoreCase(normalized)) {
                return v;
            }
        }
        String supported =
                Arrays.stream(enumClass.getEnumConstants())
                        .map(e -> e.name().toLowerCase(Locale.ROOT))
                        .reduce((a, b) -> a + ", " + b)
                        .orElse("");
        throw new FileConnectorException(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                "Unsupported " + optionKey + ": [" + rawValue + "], supported: " + supported + ".");
    }
}
