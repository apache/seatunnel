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

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.FileReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKFileAggCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKFileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ClickhouseFileSinkAggCommitter
        implements SinkAggregatedCommitter<CKFileCommitInfo, CKFileAggCommitInfo> {

    private transient ClickhouseProxy proxy;
    private final ClickhouseTable clickhouseTable;

    private final FileReaderOption fileReaderOption;

    public ClickhouseFileSinkAggCommitter(FileReaderOption readerOption) {
        fileReaderOption = readerOption;
        proxy = new ClickhouseProxy(readerOption.getShardMetadata().getDefaultShard().getNode());
        clickhouseTable =
                proxy.getClickhouseTable(
                        proxy.getClickhouseConnection(),
                        readerOption.getShardMetadata().getDatabase(),
                        readerOption.getShardMetadata().getTable());
    }

    @Override
    public List<CKFileAggCommitInfo> commit(List<CKFileAggCommitInfo> aggregatedCommitInfo)
            throws IOException {
        aggregatedCommitInfo.forEach(
                commitInfo ->
                        commitInfo
                                .getDetachedFiles()
                                .forEach(
                                        (shard, files) -> {
                                            try {
                                                this.attachFileToClickhouse(shard, files);
                                            } catch (ClickHouseException e) {
                                                throw new SeaTunnelException(
                                                        "failed commit file to clickhouse", e);
                                            }
                                        }));
        return new ArrayList<>();
    }

    @Override
    public CKFileAggCommitInfo combine(List<CKFileCommitInfo> commitInfos) {
        Map<Shard, List<String>> files = new HashMap<>();
        commitInfos.forEach(
                infos ->
                        infos.getDetachedFiles()
                                .forEach(
                                        (shard, file) -> {
                                            if (files.containsKey(shard)) {
                                                files.get(shard).addAll(file);
                                            } else {
                                                files.put(shard, file);
                                            }
                                        }));
        return new CKFileAggCommitInfo(files);
    }

    @Override
    public void abort(List<CKFileAggCommitInfo> aggregatedCommitInfo) throws Exception {}

    private ClickhouseProxy getProxy() {
        if (proxy != null) {
            return proxy;
        }
        synchronized (this) {
            if (proxy != null) {
                return proxy;
            }
            proxy =
                    new ClickhouseProxy(
                            fileReaderOption.getShardMetadata().getDefaultShard().getNode());
            return proxy;
        }
    }

    @Override
    public void close() throws IOException {
        if (proxy != null) {
            proxy.close();
        }
    }

    private void attachFileToClickhouse(Shard shard, List<String> clickhouseLocalFiles)
            throws ClickHouseException {
        ClickHouseRequest<?> request = getProxy().getClickhouseConnection(shard);
        for (String clickhouseLocalFile : clickhouseLocalFiles) {
            String attachSql =
                    String.format(
                            "ALTER TABLE %s ATTACH PART '%s'",
                            clickhouseTable.getLocalTableName(),
                            clickhouseLocalFile.substring(
                                    clickhouseLocalFile.lastIndexOf("/") + 1));

            log.info("Attach file to clickhouse table: {}", attachSql);
            ClickHouseResponse response = request.query(attachSql).executeAndWait();
            response.close();
        }
    }
}
