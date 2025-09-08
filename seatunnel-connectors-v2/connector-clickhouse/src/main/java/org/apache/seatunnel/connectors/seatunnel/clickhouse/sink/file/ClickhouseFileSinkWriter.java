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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.FileReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.ShardRouter;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKFileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSinkState;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;

import org.apache.commons.io.FileUtils;

import com.clickhouse.client.ClickHouseRequest;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class ClickhouseFileSinkWriter
        implements SinkWriter<SeaTunnelRow, CKFileCommitInfo, ClickhouseSinkState> {

    private static final String CK_LOCAL_CONFIG_TEMPLATE =
            "<yandex><path> %s </path> <users><default><password/> <profile>default</profile> <quota>default</quota>"
                    + "<access_management>1</access_management></default></users><profiles><default/></profiles><quotas><default/></quotas></yandex>";
    private static final String CLICKHOUSE_SETTINGS_KEY = "SETTINGS";
    private static final String CLICKHOUSE_DDL_SETTING_FILTER = "storage_policy";
    private static final String CLICKHOUSE_LOCAL_FILE_SUFFIX = "/local_data.log";
    private static final int UUID_LENGTH = 10;
    private final FileReaderOption readerOption;
    private final ShardRouter shardRouter;
    private final ClickhouseProxy proxy;
    private final ClickhouseTable clickhouseTable;
    private final Map<Shard, List<String>> shardLocalDataPaths;
    private final Map<Shard, FileChannel> rowCache;
    private final Map<Shard, MappedByteBuffer> bufferCache;
    private final Integer bufferSize = 1024 * 128;

    private final Map<Shard, String> shardTempFile;

    private final SinkWriter.Context context;
    private final ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();

    public ClickhouseFileSinkWriter(FileReaderOption readerOption, SinkWriter.Context context) {
        this.readerOption = readerOption;
        this.context = context;
        proxy =
                new ClickhouseProxy(
                        this.readerOption.getShardMetadata().getDefaultShard().getNode());
        shardRouter = new ShardRouter(proxy, this.readerOption.getShardMetadata());
        clickhouseTable =
                proxy.getClickhouseTable(
                        proxy.getClickhouseConnection(),
                        this.readerOption.getShardMetadata().getDatabase(),
                        this.readerOption.getShardMetadata().getTable());
        rowCache = new HashMap<>(Common.COLLECTION_SIZE);
        bufferCache = new HashMap<>(Common.COLLECTION_SIZE);
        shardTempFile = new HashMap<>();
        nodePasswordCheck();

        // find file local save path of each node
        shardLocalDataPaths =
                shardRouter.getShards().values().stream()
                        .collect(
                                Collectors.toMap(
                                        Function.identity(),
                                        shard -> {
                                            ClickHouseRequest<?> request =
                                                    proxy.getClickhouseConnection(shard);
                                            ClickhouseTable shardTable =
                                                    proxy.getClickhouseTable(
                                                            request,
                                                            shard.getNode().getDatabase().get(),
                                                            clickhouseTable.getLocalTableName());
                                            return shardTable.getDataPaths();
                                        }));
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        Shard shard = shardRouter.getShard(element);
        FileChannel channel =
                rowCache.computeIfAbsent(
                        shard,
                        k -> {
                            String uuid =
                                    UUID.randomUUID()
                                            .toString()
                                            .substring(0, UUID_LENGTH)
                                            .replaceAll("-", "_");
                            String clickhouseLocalFile =
                                    String.format("%s/%s", readerOption.getFileTempPath(), uuid);
                            try {
                                FileUtils.forceMkdir(new File(clickhouseLocalFile));
                                String clickhouseLocalFileTmpFile =
                                        clickhouseLocalFile + CLICKHOUSE_LOCAL_FILE_SUFFIX;
                                shardTempFile.put(shard, clickhouseLocalFileTmpFile);
                                return FileChannel.open(
                                        Paths.get(clickhouseLocalFileTmpFile),
                                        StandardOpenOption.WRITE,
                                        StandardOpenOption.READ,
                                        StandardOpenOption.CREATE_NEW);
                            } catch (IOException e) {
                                throw CommonError.fileOperationFailed(
                                        "ClickhouseFile", "write", clickhouseLocalFile, e);
                            }
                        });
        saveDataToFile(channel, element, shard);
    }

    private void nodePasswordCheck() {
        if (!this.readerOption.isNodeFreePass()) {
            shardRouter
                    .getShards()
                    .values()
                    .forEach(
                            shard -> {
                                if (!this.readerOption
                                                .getNodePassword()
                                                .containsKey(
                                                        shard.getNode().getAddress().getHostName())
                                        && !this.readerOption
                                                .getNodePassword()
                                                .containsKey(shard.getNode().getHost())) {
                                    throw new ClickhouseConnectorException(
                                            ClickhouseConnectorErrorCode
                                                    .PASSWORD_NOT_FOUND_IN_SHARD_NODE,
                                            "Cannot find password of shard "
                                                    + shard.getNode().getAddress().getHostName());
                                }
                            });
        }
    }

    @Override
    public Optional<CKFileCommitInfo> prepareCommit() throws IOException {
        for (FileChannel channel : rowCache.values()) {
            channel.close();
        }
        Map<Shard, List<String>> detachedFiles = new HashMap<>();
        shardTempFile.forEach(
                (shard, path) -> {
                    List<String> clickhouseLocalFiles = null;
                    try {
                        clickhouseLocalFiles = generateClickhouseLocalFiles(path);
                        // move file to server
                        moveClickhouseLocalFileToServer(shard, clickhouseLocalFiles);
                        detachedFiles.put(shard, clickhouseLocalFiles);
                    } catch (Exception e) {
                        throw new ClickhouseConnectorException(
                                CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                                "Flush data into clickhouse file error",
                                e);
                    } finally {
                        if (clickhouseLocalFiles != null && !clickhouseLocalFiles.isEmpty()) {
                            // clear local file
                            clearLocalFileDirectory(clickhouseLocalFiles);
                        }
                    }
                });
        rowCache.clear();
        shardTempFile.clear();
        return Optional.of(new CKFileCommitInfo(detachedFiles));
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        for (FileChannel channel : rowCache.values()) {
            channel.close();
        }
    }

    private void saveDataToFile(FileChannel fileChannel, SeaTunnelRow row, Shard shard)
            throws IOException {
        String data =
                this.readerOption.getFields().stream()
                                .map(
                                        field -> {
                                            Object fieldValueObj =
                                                    row.getField(
                                                            this.readerOption
                                                                    .getSeaTunnelRowType()
                                                                    .indexOf(field));
                                            if (fieldValueObj == null) {
                                                return "";
                                            } else {
                                                return fieldValueObj.toString();
                                            }
                                        })
                                .collect(Collectors.joining(readerOption.getFileFieldsDelimiter()))
                        + "\n";

        MappedByteBuffer buffer =
                bufferCache.computeIfAbsent(
                        shard,
                        k -> {
                            try {
                                return fileChannel.map(
                                        FileChannel.MapMode.READ_WRITE, 0, bufferSize);
                            } catch (IOException e) {
                                throw CommonError.fileOperationFailed(
                                        "ClickhouseFile", "write", "UNKNOWN", e);
                            }
                        });
        byte[] byteData = data.getBytes(StandardCharsets.UTF_8);
        if (buffer.position() + byteData.length > buffer.capacity()) {
            buffer =
                    fileChannel.map(FileChannel.MapMode.READ_WRITE, fileChannel.size(), bufferSize);
            bufferCache.put(shard, buffer);
        }
        buffer.put(byteData);
    }

    private List<String> generateClickhouseLocalFiles(String clickhouseLocalFileTmpFile)
            throws IOException, InterruptedException {
        // temp file path format prefix/<uuid>/suffix
        String[] tmpStrArr = clickhouseLocalFileTmpFile.split("/");
        String uuid = tmpStrArr[tmpStrArr.length - 2];
        List<String> localPaths =
                Arrays.stream(this.readerOption.getClickhouseLocalPath().trim().split(" "))
                        .collect(Collectors.toList());
        String clickhouseLocalFile =
                clickhouseLocalFileTmpFile.substring(
                        0,
                        clickhouseLocalFileTmpFile.length()
                                - CLICKHOUSE_LOCAL_FILE_SUFFIX.length());
        List<String> command = new ArrayList<>(localPaths);
        if (localPaths.size() == 1) {
            command.add("local");
        }
        command.add("--file");
        command.add(clickhouseLocalFileTmpFile);
        command.add("--format_csv_delimiter");
        command.add("\"" + readerOption.getFileFieldsDelimiter() + "\"");
        command.add("-S");
        command.add(
                "\""
                        + this.readerOption.getFields().stream()
                                .map(
                                        field ->
                                                field
                                                        + " "
                                                        + readerOption.getTableSchema().get(field))
                                .collect(Collectors.joining(","))
                        + "\"");
        command.add("-N");
        command.add("\"" + "temp_table" + uuid + "\"");
        command.add("-d _local");
        command.add("-n");
        command.add("-q");
        command.add(
                String.format(
                        "\"%s; INSERT INTO TABLE %s SELECT %s FROM temp_table%s;\"",
                        adjustClickhouseDDL(),
                        clickhouseTable.getLocalTableName(),
                        readerOption.getTableSchema().keySet().stream()
                                .map(
                                        s -> {
                                            if (readerOption.getFields().contains(s)) {
                                                return s;
                                            } else {
                                                return "NULL";
                                            }
                                        })
                                .collect(Collectors.joining(",")),
                        uuid));
        if (readerOption.isCompatibleMode()) {
            String ckLocalConfigPath =
                    String.format("%s/%s/config.xml", readerOption.getFileTempPath(), uuid);
            try (FileWriter writer = new FileWriter(ckLocalConfigPath)) {
                writer.write(String.format(CK_LOCAL_CONFIG_TEMPLATE, clickhouseLocalFile));
            } catch (IOException e) {
                throw CommonError.fileOperationFailed(
                        "ClickhouseFile", "write", clickhouseLocalFile, e);
            }
            command.add("--config-file");
            command.add("\"" + ckLocalConfigPath + "\"");
        } else {
            command.add("--path");
            command.add("\"" + clickhouseLocalFile + "\"");
        }
        log.info("Generate clickhouse local file command: {}", String.join(" ", command));
        ProcessBuilder processBuilder = new ProcessBuilder("bash", "-c", String.join(" ", command));
        Process start = processBuilder.start();
        // we just wait for the process to finish
        try (InputStream inputStream = start.getInputStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                log.info(line);
            }
        }
        try (InputStream inputStream = start.getErrorStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                log.error(line);
            }
        }
        start.waitFor();
        File file =
                new File(
                        clickhouseLocalFile
                                + "/data/_local/"
                                + clickhouseTable.getLocalTableName());
        if (!file.exists()) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.FILE_NOT_EXISTS,
                    "clickhouse local file not exists");
        }
        File[] files = file.listFiles();
        if (files == null) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.FILE_NOT_EXISTS,
                    "clickhouse local file not exists");
        }
        return Arrays.stream(files)
                .filter(File::isDirectory)
                .filter(f -> !"detached".equals(f.getName()))
                .map(
                        f -> {
                            File newFile =
                                    new File(
                                            f.getParent()
                                                    + "/"
                                                    + f.getName()
                                                    + "_"
                                                    + context.getIndexOfSubtask());
                            if (f.renameTo(newFile)) {
                                return newFile;
                            } else {
                                log.warn(
                                        "rename file failed, will continue move file, but maybe cause file conflict");
                                return f;
                            }
                        })
                .map(File::getAbsolutePath)
                .collect(Collectors.toList());
    }

    private void moveClickhouseLocalFileToServer(Shard shard, List<String> clickhouseLocalFiles) {
        String hostAddress = shard.getNode().getHost();
        String user = readerOption.getNodeUser().getOrDefault(hostAddress, "root");
        String password = readerOption.getNodePassword().getOrDefault(hostAddress, null);
        String keyPath = readerOption.getKeyPath();
        FileTransfer fileTransfer =
                FileTransferFactory.createFileTransfer(
                        this.readerOption.getCopyMethod(), hostAddress, user, password, keyPath);
        fileTransfer.init();
        int randomPath = threadLocalRandom.nextInt(shardLocalDataPaths.get(shard).size());
        fileTransfer.transferAndChown(
                clickhouseLocalFiles, shardLocalDataPaths.get(shard).get(randomPath) + "detached/");
        fileTransfer.close();
    }

    private void clearLocalFileDirectory(List<String> clickhouseLocalFiles) {
        String clickhouseLocalFile = clickhouseLocalFiles.get(0);
        String localFileDir =
                clickhouseLocalFile.substring(
                        0, readerOption.getFileTempPath().length() + UUID_LENGTH + 1);
        try {
            File file = new File(localFileDir);
            if (file.exists()) {
                FileUtils.deleteDirectory(file);
            }
        } catch (IOException e) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.DELETE_DIRECTORY_FIELD,
                    "Unable to delete directory " + localFileDir,
                    e);
        }
    }

    private String adjustClickhouseDDL() {
        String createTableDDL =
                clickhouseTable
                        .getCreateTableDDL()
                        .replace(clickhouseTable.getDatabase() + ".", "")
                        .replaceAll("`", "");
        if (createTableDDL.contains(CLICKHOUSE_SETTINGS_KEY)) {
            List<String> filters =
                    Arrays.stream(CLICKHOUSE_DDL_SETTING_FILTER.split(","))
                            .collect(Collectors.toList());
            int p = createTableDDL.indexOf(CLICKHOUSE_SETTINGS_KEY);
            String filteredSetting =
                    Arrays.stream(
                                    createTableDDL
                                            .substring(p + CLICKHOUSE_SETTINGS_KEY.length())
                                            .split(","))
                            .filter(e -> !filters.contains(e.split("=")[0].trim()))
                            .collect(Collectors.joining(","));
            createTableDDL =
                    createTableDDL.substring(0, p) + CLICKHOUSE_SETTINGS_KEY + filteredSetting;
        }
        return createTableDDL;
    }
}
