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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.DistributedEngine;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseTable;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@SuppressWarnings("magicnumber")
public class ClickhouseProxy {

    private final ClickHouseRequest<?> clickhouseRequest;
    private final ClickHouseClient client;

    private final Map<Shard, ClickHouseClient> shardToDataSource = new ConcurrentHashMap<>(16);

    public ClickhouseProxy(ClickHouseNode node) {
        this.client = ClickHouseClient.newInstance(node.getProtocol());
        this.clickhouseRequest =
            client.connect(node).format(ClickHouseFormat.RowBinaryWithNamesAndTypes);

    }

    public ClickHouseRequest<?> getClickhouseConnection() {
        return this.clickhouseRequest;
    }

    public ClickHouseRequest<?> getClickhouseConnection(Shard shard) {
        ClickHouseClient c = shardToDataSource
            .computeIfAbsent(shard, s -> ClickHouseClient.newInstance(s.getNode().getProtocol()));
        return c.connect(shard.getNode()).format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
    }

    public DistributedEngine getClickhouseDistributedTable(String database, String table) {
        ClickHouseRequest<?> request = getClickhouseConnection();
        return getClickhouseDistributedTable(request, database, table);
    }

    public DistributedEngine getClickhouseDistributedTable(ClickHouseRequest<?> connection, String database,
                                                           String table) {
        String sql = String.format("select engine_full from system.tables where database = '%s' and name = '%s' and engine = 'Distributed'", database, table);
        try (ClickHouseResponse response = connection.query(sql).executeAndWait()) {
            List<ClickHouseRecord> records = response.stream().collect(Collectors.toList());
            if (!records.isEmpty()) {
                ClickHouseRecord record = records.get(0);
                // engineFull field will be like : Distributed(cluster, database, table[, sharding_key[, policy_name]])
                String engineFull = record.getValue(0).asString();
                List<String> infos = Arrays.stream(engineFull.substring(12).split(","))
                    .map(s -> s.replace("'", "").trim()).collect(Collectors.toList());
                return new DistributedEngine(infos.get(0), infos.get(1), infos.get(2).replace("\\)", "").trim());
            }
            throw new ClickhouseConnectorException(SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED, "Cannot get distributed table from clickhouse, resultSet is empty");
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED, "Cannot get distributed table from clickhouse", e);
        }
    }

    /**
     * Get ClickHouse table schema, the key is fileName, value is value type.
     *
     * @param table table name.
     * @return schema map.
     */
    public Map<String, String> getClickhouseTableSchema(String table) {
        ClickHouseRequest<?> request = getClickhouseConnection();
        return getClickhouseTableSchema(request, table);
    }

    public Map<String, String> getClickhouseTableSchema(ClickHouseRequest<?> request, String table) {
        String sql = "desc " + table;
        Map<String, String> schema = new LinkedHashMap<>();
        try (ClickHouseResponse response = request.query(sql).executeAndWait()) {
            response.records().forEach(r -> schema.put(r.getValue(0).asString(), r.getValue(1).asString()));
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(CommonErrorCode.TABLE_SCHEMA_GET_FAILED, "Cannot get table schema from clickhouse", e);
        }
        return schema;
    }

    /**
     * Get the shard of the given cluster.
     *
     * @param connection  clickhouse connection.
     * @param clusterName cluster name.
     * @param database    database of the shard.
     * @param port        port of the shard.
     * @return shard list.
     */
    public List<Shard> getClusterShardList(ClickHouseRequest<?> connection, String clusterName,
                                           String database, int port, String username, String password) {
        String sql = "select shard_num,shard_weight,replica_num,host_name,host_address,port from system.clusters where cluster = '" + clusterName + "'";
        List<Shard> shardList = new ArrayList<>();
        try (ClickHouseResponse response = connection.query(sql).executeAndWait()) {
            response.records().forEach(r -> {
                shardList.add(new Shard(
                    r.getValue(0).asInteger(),
                    r.getValue(1).asInteger(),
                    r.getValue(2).asInteger(),
                    r.getValue(3).asString(),
                    r.getValue(4).asString(),
                    port, database, username, password));
            });
            return shardList;
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(ClickhouseConnectorErrorCode.CLUSTER_LIST_GET_FAILED, "Cannot get cluster shard list from clickhouse", e);
        }
    }

    /**
     * Get ClickHouse table info.
     *
     * @param database database of the table.
     * @param table    table name of the table.
     * @return clickhouse table info.
     */
    public ClickhouseTable getClickhouseTable(String database, String table) {
        String sql = String.format("select engine,create_table_query,engine_full,data_paths from system.tables where database = '%s' and name = '%s'", database, table);
        try (ClickHouseResponse response = clickhouseRequest.query(sql).executeAndWait()) {
            List<ClickHouseRecord> records = response.stream().collect(Collectors.toList());
            if (records.isEmpty()) {
                throw new ClickhouseConnectorException(SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED, "Cannot get table from clickhouse, resultSet is empty");
            }
            ClickHouseRecord record = records.get(0);
            String engine = record.getValue(0).asString();
            String createTableDDL = record.getValue(1).asString();
            String engineFull = record.getValue(2).asString();
            List<String> dataPaths = record.getValue(3).asTuple().stream().map(Object::toString).collect(Collectors.toList());
            DistributedEngine distributedEngine = null;
            if ("Distributed".equals(engine)) {
                distributedEngine = getClickhouseDistributedTable(clickhouseRequest, database, table);
                String localTableSQL = String.format("select engine,create_table_query from system.tables where database = '%s' and name = '%s'",
                    distributedEngine.getDatabase(), distributedEngine.getTable());
                try (ClickHouseResponse rs = clickhouseRequest.query(localTableSQL).executeAndWait()) {
                    List<ClickHouseRecord> localTableRecords = rs.stream().collect(Collectors.toList());
                    if (localTableRecords.isEmpty()) {
                        throw new ClickhouseConnectorException(SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED, "Cannot get table from clickhouse, resultSet is empty");
                    }
                    String localEngine = localTableRecords.get(0).getValue(0).asString();
                    String createLocalTableDDL = localTableRecords.get(0).getValue(1).asString();
                    createTableDDL = localizationEngine(localEngine, createLocalTableDDL);
                }
            }
            return new ClickhouseTable(
                database,
                table,
                distributedEngine,
                engine,
                createTableDDL,
                engineFull,
                dataPaths,
                getClickhouseTableSchema(clickhouseRequest, table));
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED, "Cannot get clickhouse table", e);
        }

    }

    /**
     * Localization the engine in clickhouse local table's createTableDDL to support specific engine.
     * For example: change ReplicatedMergeTree to MergeTree.
     *
     * @param engine original engine of clickhouse local table
     * @param ddl    createTableDDL of clickhouse local table
     * @return createTableDDL of clickhouse local table which can support specific engine
     * TODO: support more engine
     */
    public String localizationEngine(String engine, String ddl) {
        if ("ReplicatedMergeTree".equalsIgnoreCase(engine)) {
            return ddl.replaceAll("ReplicatedMergeTree(\\([^\\)]*\\))", "MergeTree()");
        } else {
            return ddl;
        }
    }

    public void close() {
        if (this.client != null) {
            this.client.close();
        }
        shardToDataSource.values().forEach(ClickHouseClient::close);
    }
}
