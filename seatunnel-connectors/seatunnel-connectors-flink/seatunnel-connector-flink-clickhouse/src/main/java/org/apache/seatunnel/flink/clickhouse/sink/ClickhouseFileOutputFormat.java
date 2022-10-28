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

package org.apache.seatunnel.flink.clickhouse.sink;

import static org.apache.seatunnel.flink.clickhouse.ConfigKey.CLICKHOUSE_LOCAL_PATH;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.COPY_METHOD;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.DATABASE;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.NODE_ADDRESS;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.NODE_FREE_PASSWORD;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.NODE_PASS;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.PASSWORD;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.TABLE;

import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.flink.clickhouse.pojo.ClickhouseFileCopyMethod;
import org.apache.seatunnel.flink.clickhouse.pojo.Shard;
import org.apache.seatunnel.flink.clickhouse.pojo.ShardMetadata;
import org.apache.seatunnel.flink.clickhouse.sink.client.ClickhouseClient;
import org.apache.seatunnel.flink.clickhouse.sink.client.ShardRouter;
import org.apache.seatunnel.flink.clickhouse.sink.file.ClickhouseTable;
import org.apache.seatunnel.flink.clickhouse.sink.file.FileTransfer;
import org.apache.seatunnel.flink.clickhouse.sink.file.ScpFileTransfer;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flink.types.Row;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;

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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class ClickhouseFileOutputFormat {

    private static final String CLICKHOUSE_LOCAL_FILE_PREFIX = "/tmp/clickhouse-local/flink-file";
    private static final int UUID_LENGTH = 10;

    private final Config config;
    private final String clickhouseLocalPath;
    private final List<String> fields;
    private final ShardMetadata shardMetadata;
    private final ClickhouseFileCopyMethod clickhouseFileCopyMethod;
    private final Map<String, String> nodePassword;

    private final ClickhouseClient clickhouseClient;
    private final ShardRouter shardRouter;
    private final ClickhouseTable clickhouseTable;
    private final Map<String, String> schemaMap;
    private final Map<Shard, List<String>> shardLocalDataPaths;

    // In most of the case, the data has been already shuffled in ClickhouseFileBatchSink#outputBatch
    private final Map<Shard, List<Row>> rowCache;

    public ClickhouseFileOutputFormat(Config config, ShardMetadata shardMetadata, List<String> fields) throws IOException {
        this.config = config;
        this.clickhouseLocalPath = config.getString(CLICKHOUSE_LOCAL_PATH);
        this.shardMetadata = shardMetadata;
        this.fields = fields;
        this.clickhouseFileCopyMethod = ClickhouseFileCopyMethod.from(config.getString(COPY_METHOD));
        if (TypesafeConfigUtils.getConfig(config, NODE_FREE_PASSWORD, true)) {
            this.nodePassword = Collections.emptyMap();
        } else {
            nodePassword = config.getObjectList(NODE_PASS).stream()
                .collect(Collectors.toMap(
                    configObject -> configObject.toConfig().getString(NODE_ADDRESS),
                    configObject -> configObject.toConfig().getString(PASSWORD)));
        }
        clickhouseClient = new ClickhouseClient(config);
        shardRouter = new ShardRouter(clickhouseClient, shardMetadata);
        clickhouseTable = clickhouseClient.getClickhouseTable(config.getString(DATABASE), config.getString(TABLE));
        schemaMap = clickhouseClient.getClickhouseTableSchema(config.getString(TABLE));

        rowCache = new HashMap<>(shardRouter.getShards().keySet().size());
        if (!TypesafeConfigUtils.getConfig(config, NODE_FREE_PASSWORD, true)) {
            shardRouter.getShards().values().forEach(shard -> {
                if (!nodePassword.containsKey(shard.getHostAddress()) && !nodePassword.containsKey(shard.getHostname())) {
                    throw new RuntimeException("Cannot find password of shard " + shard.getHostAddress());
                }
            });
        }
        shardLocalDataPaths = shardRouter.getShards().values().stream()
            .collect(Collectors.toMap(
                Function.identity(),
                shard -> {
                    ClickhouseTable shardTable = clickhouseClient
                        .getClickhouseTable(shard.getDatabase(), clickhouseTable.getLocalTableName());
                    return shardTable.getDataPaths();
                }));
    }

    public void writeRecords(Iterable<Row> records) {
        for (Row record : records) {
            Shard shard = shardRouter.getShard(record);
            rowCache.computeIfAbsent(shard, k -> new ArrayList<>()).add(record);
        }
        for (Map.Entry<Shard, List<Row>> entry : rowCache.entrySet()) {
            Shard shard = entry.getKey();
            List<Row> rows = entry.getValue();
            flush(shard, rows);
            rows.clear();
        }
    }

    private void flush(Shard shard, List<Row> rows) {
        try {
            // generate clickhouse local file
            List<String> clickhouseLocalFiles = generateClickhouseLocalFiles(shard, rows);
            // move file to server
            attachClickhouseLocalFileToServer(shard, clickhouseLocalFiles);
            // clear local file
            clearLocalFileDirectory(clickhouseLocalFiles);
        } catch (Exception e) {
            throw new RuntimeException("Flush data into clickhouse file error", e);
        }
    }

    private List<String> generateClickhouseLocalFiles(Shard shard, List<Row> rows) throws IOException, InterruptedException {
        if (CollectionUtils.isEmpty(rows)) {
            return Collections.emptyList();
        }
        String uuid = UUID.randomUUID().toString().substring(0, UUID_LENGTH).replaceAll("-", "_");
        String clickhouseLocalFile = String.format("%s/%s", CLICKHOUSE_LOCAL_FILE_PREFIX, uuid);
        FileUtils.forceMkdir(new File(clickhouseLocalFile));
        String clickhouseLocalFileTmpFile = clickhouseLocalFile + "/local_data.log";
        FileChannel fileChannel = FileChannel.open(Paths.get(clickhouseLocalFileTmpFile), StandardOpenOption.WRITE,
            StandardOpenOption.READ, StandardOpenOption.CREATE_NEW);
        String data = rows.stream()
            .map(row -> fields.stream().map(field -> row.getField(field).toString())
                .collect(Collectors.joining("\t")))
            .collect(Collectors.joining("\n"));
        MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, fileChannel.size(),
            data.getBytes(StandardCharsets.UTF_8).length);
        buffer.put(data.getBytes(StandardCharsets.UTF_8));

        List<String> command = new ArrayList<>();
        command.add("cat");
        command.add(clickhouseLocalFileTmpFile);
        command.add("|");

        command.addAll(Arrays.stream(clickhouseLocalPath.trim().split(" ")).collect(Collectors.toList()));
        command.add("local");
        command.add("-S");
        command.add("\"" + fields.stream().map(field -> field + " " + schemaMap.get(field)).collect(Collectors.joining(",")) + "\"");
        command.add("-N");
        command.add("\"" + "temp_table" + uuid + "\"");
        command.add("-q");
        command.add(String.format(
            "\"%s; INSERT INTO TABLE %s SELECT %s FROM temp_table%s;\"",
            clickhouseTable.getCreateTableDDL().replace(clickhouseTable.getDatabase() + ".", "").replaceAll("`", ""),
            clickhouseTable.getLocalTableName(),
            schemaMap.entrySet().stream().map(entry -> {
                if (fields.contains(entry.getKey())) {
                    return entry.getKey();
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
            throw new RuntimeException("clickhouse local file not exists");
        }
        File[] files = file.listFiles();
        if (files == null) {
            throw new RuntimeException("clickhouse local file not exists");
        }
        return Arrays.stream(files)
            .filter(File::isDirectory)
            .filter(f -> !"detached".equals(f.getName()))
            .map(File::getAbsolutePath).collect(Collectors.toList());
    }

    private void attachClickhouseLocalFileToServer(Shard shard, List<String> clickhouseLocalFiles) {
        if (ClickhouseFileCopyMethod.SCP.equals(clickhouseFileCopyMethod)) {
            String hostAddress = shard.getHostAddress();
            String password = nodePassword.getOrDefault(hostAddress, null);
            FileTransfer fileTransfer = new ScpFileTransfer(hostAddress, password);
            fileTransfer.init();
            fileTransfer.transferAndChown(clickhouseLocalFiles, shardLocalDataPaths.get(shard).get(0) + "detached/");
            fileTransfer.close();
        } else {
            throw new RuntimeException("unsupported clickhouse file copy method " + clickhouseFileCopyMethod);
        }

        try (ClickHouseConnectionImpl clickhouseConnection = clickhouseClient.getClickhouseConnection(shard)) {
            for (String clickhouseLocalFile : clickhouseLocalFiles) {
                clickhouseConnection.createStatement()
                    .execute(String.format("ALTER TABLE %s ATTACH PART '%s'",
                        clickhouseTable.getLocalTableName(),
                        clickhouseLocalFile.substring(clickhouseLocalFile.lastIndexOf("/") + 1)));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Unable to close connection", e);
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
            throw new RuntimeException("Unable to delete directory " + localFileDir, e);
        }
    }

}
