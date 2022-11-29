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
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.FileReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.ShardRouter;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSinkState;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class ClickhouseFileSinkWriter implements SinkWriter<SeaTunnelRow, CKCommitInfo, ClickhouseSinkState> {
    private static final String CLICKHOUSE_LOCAL_FILE_PREFIX = "/tmp/clickhouse-local/seatunnel-file";
    private static final int UUID_LENGTH = 10;
    private final FileReaderOption readerOption;
    private final ShardRouter shardRouter;
    private final ClickhouseProxy proxy;
    private final ClickhouseTable clickhouseTable;
    private final Map<Shard, List<String>> shardLocalDataPaths;
    private final Map<Shard, List<SeaTunnelRow>> rowCache;

    public ClickhouseFileSinkWriter(FileReaderOption readerOption, SinkWriter.Context context) {
        this.readerOption = readerOption;
        proxy = new ClickhouseProxy(this.readerOption.getShardMetadata().getDefaultShard().getNode());
        shardRouter = new ShardRouter(proxy, this.readerOption.getShardMetadata());
        clickhouseTable = proxy.getClickhouseTable(this.readerOption.getShardMetadata().getDatabase(),
            this.readerOption.getShardMetadata().getTable());
        rowCache = new HashMap<>(Common.COLLECTION_SIZE);

        nodePasswordCheck();

        // find file local save path of each node
        shardLocalDataPaths = shardRouter.getShards().values().stream()
            .collect(Collectors.toMap(Function.identity(), shard -> {
                ClickhouseTable shardTable = proxy.getClickhouseTable(shard.getNode().getDatabase().get(),
                    clickhouseTable.getLocalTableName());
                return shardTable.getDataPaths();
            }));
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        Shard shard = shardRouter.getShard(element);
        rowCache.computeIfAbsent(shard, k -> new ArrayList<>()).add(element);
    }

    private void nodePasswordCheck() {
        if (!this.readerOption.isNodeFreePass()) {
            shardRouter.getShards().values().forEach(shard -> {
                if (!this.readerOption.getNodePassword().containsKey(shard.getNode().getAddress().getHostName())
                    && !this.readerOption.getNodePassword().containsKey(shard.getNode().getHost())) {
                    throw new ClickhouseConnectorException(ClickhouseConnectorErrorCode.PASSWORD_NOT_FOUND_IN_SHARD_NODE, "Cannot find password of shard " + shard.getNode().getAddress().getHostName());
                }
            });
        }
    }

    @Override
    public Optional<CKCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {

    }

    @Override
    public void close() throws IOException {
        rowCache.forEach(this::flush);
    }

    private void flush(Shard shard, List<SeaTunnelRow> rows) {
        try {
            // generate clickhouse local file
            // TODO generate file by sub rows to save memory
            List<String> clickhouseLocalFiles = generateClickhouseLocalFiles(rows);
            // move file to server
            attachClickhouseLocalFileToServer(shard, clickhouseLocalFiles);
            // clear local file
            clearLocalFileDirectory(clickhouseLocalFiles);
        } catch (Exception e) {
            throw new ClickhouseConnectorException(CommonErrorCode.FLUSH_DATA_FAILED, "Flush data into clickhouse file error", e);
        }
    }

    private List<String> generateClickhouseLocalFiles(List<SeaTunnelRow> rows) throws IOException,
        InterruptedException {
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }
        String uuid = UUID.randomUUID().toString().substring(0, UUID_LENGTH).replaceAll("-", "_");
        String clickhouseLocalFile = String.format("%s/%s", CLICKHOUSE_LOCAL_FILE_PREFIX, uuid);
        FileUtils.forceMkdir(new File(clickhouseLocalFile));
        String clickhouseLocalFileTmpFile = clickhouseLocalFile + "/local_data.log";
        try (FileChannel fileChannel = FileChannel.open(Paths.get(clickhouseLocalFileTmpFile), StandardOpenOption.WRITE,
            StandardOpenOption.READ, StandardOpenOption.CREATE_NEW)) {
            String data = rows.stream()
                .map(row -> this.readerOption.getFields().stream().map(field -> row.getField(this.readerOption.getSeaTunnelRowType().indexOf(field)).toString())
                    .collect(Collectors.joining("\t")))
                .collect(Collectors.joining("\n"));
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, fileChannel.size(),
                data.getBytes(StandardCharsets.UTF_8).length);
            buffer.put(data.getBytes(StandardCharsets.UTF_8));
        }

        List<String> localPaths = Arrays.stream(this.readerOption.getClickhouseLocalPath().trim().split(" "))
            .collect(Collectors.toList());
        List<String> command = new ArrayList<>(localPaths);
        if (localPaths.size() == 1) {
            command.add("local");
        }
        command.add("--file");
        command.add(clickhouseLocalFileTmpFile);
        command.add("-S");
        command.add("\"" + this.readerOption.getFields().stream().map(field -> field + " " + readerOption.getTableSchema().get(field)).collect(Collectors.joining(",")) + "\"");
        command.add("-N");
        command.add("\"" + "temp_table" + uuid + "\"");
        command.add("-q");
        command.add(String.format(
            "\"%s; INSERT INTO TABLE %s SELECT %s FROM temp_table%s;\"",
            clickhouseTable.getCreateTableDDL().replace(clickhouseTable.getDatabase() + ".", "").replaceAll("`", ""),
            clickhouseTable.getLocalTableName(),
            readerOption.getTableSchema().keySet().stream().map(s -> {
                if (readerOption.getFields().contains(s)) {
                    return s;
                } else {
                    return "NULL";
                }
            }).collect(Collectors.joining(",")),
            uuid));
        command.add("--path");
        command.add("\"" + clickhouseLocalFile + "\"");
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
        start.waitFor();
        File file = new File(clickhouseLocalFile + "/data/_local/" + clickhouseTable.getLocalTableName());
        if (!file.exists()) {
            throw new ClickhouseConnectorException(ClickhouseConnectorErrorCode.FILE_NOT_EXISTS, "clickhouse local file not exists");
        }
        File[] files = file.listFiles();
        if (files == null) {
            throw new ClickhouseConnectorException(ClickhouseConnectorErrorCode.FILE_NOT_EXISTS, "clickhouse local file not exists");
        }
        return Arrays.stream(files)
            .filter(File::isDirectory)
            .filter(f -> !"detached".equals(f.getName()))
            .map(File::getAbsolutePath).collect(Collectors.toList());
    }

    private void attachClickhouseLocalFileToServer(Shard shard, List<String> clickhouseLocalFiles) throws ClickHouseException {
        String hostAddress = shard.getNode().getAddress().getHostName();
        String user = readerOption.getNodeUser().getOrDefault(hostAddress, "root");
        String password = readerOption.getNodePassword().getOrDefault(hostAddress, null);
        FileTransfer fileTransfer = FileTransferFactory.createFileTransfer(this.readerOption.getCopyMethod(), hostAddress, user, password);
        fileTransfer.init();
        fileTransfer.transferAndChown(clickhouseLocalFiles, shardLocalDataPaths.get(shard).get(0) + "detached/");
        fileTransfer.close();
        ClickHouseRequest<?> request = proxy.getClickhouseConnection(shard);
        for (String clickhouseLocalFile : clickhouseLocalFiles) {
            ClickHouseResponse response = request.query(String.format("ALTER TABLE %s ATTACH PART '%s'",
                clickhouseTable.getLocalTableName(),
                clickhouseLocalFile.substring(clickhouseLocalFile.lastIndexOf("/") + 1))).executeAndWait();
            response.close();
        }
    }

    private void clearLocalFileDirectory(List<String> clickhouseLocalFiles) {
        String clickhouseLocalFile = clickhouseLocalFiles.get(0);
        String localFileDir = clickhouseLocalFile.substring(0, CLICKHOUSE_LOCAL_FILE_PREFIX.length() + UUID_LENGTH + 1);
        try {
            File file = new File(localFileDir);
            if (file.exists()) {
                FileUtils.deleteDirectory(new File(localFileDir));
            }
        } catch (IOException e) {
            throw new ClickhouseConnectorException(ClickhouseConnectorErrorCode.DELETE_DIRECTORY_FIELD, "Unable to delete directory " + localFileDir, e);
        }
    }

}
