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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink.client;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.timeplus.exception.TimeplusConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.timeplus.exception.TimeplusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.timeplus.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.DistributedEngine;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.file.TimeplusTable;

import com.timeplus.proton.client.ProtonClient;
import com.timeplus.proton.client.ProtonException;
import com.timeplus.proton.client.ProtonFormat;
import com.timeplus.proton.client.ProtonNode;
import com.timeplus.proton.client.ProtonRecord;
import com.timeplus.proton.client.ProtonRequest;
import com.timeplus.proton.client.ProtonResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@SuppressWarnings("magicnumber")
public class TimeplusProxy {

    private final ProtonRequest<?> ProtonRequest;
    private final ProtonClient client;

    private final Map<Shard, ProtonClient> shardToDataSource = new ConcurrentHashMap<>(16);

    public TimeplusProxy(ProtonNode node) {
        this.client = ProtonClient.newInstance(node.getProtocol());
        this.ProtonRequest = client.connect(node).format(ProtonFormat.RowBinaryWithNamesAndTypes);
    }

    public ProtonRequest<?> getProtonConnection() {
        return this.ProtonRequest;
    }

    public ProtonRequest<?> getProtonConnection(Shard shard) {
        ProtonClient c =
                shardToDataSource.computeIfAbsent(
                        shard, s -> ProtonClient.newInstance(s.getNode().getProtocol()));
        return c.connect(shard.getNode()).format(ProtonFormat.RowBinaryWithNamesAndTypes);
    }

    public DistributedEngine getProtonDistributedTable(
            ProtonRequest<?> connection, String database, String table) {
        String sql =
                String.format(
                        "select engine_full from system.tables where database = '%s' and name = '%s' and engine = 'Distributed'",
                        database, table);
        try (ProtonResponse response = connection.query(sql).executeAndWait()) {
            List<ProtonRecord> records = response.stream().collect(Collectors.toList());
            if (!records.isEmpty()) {
                ProtonRecord record = records.get(0);
                // engineFull field will be like : Distributed(cluster, database, table[,
                // sharding_key[, policy_name]])
                String engineFull = record.getValue(0).asString();
                List<String> infos =
                        Arrays.stream(engineFull.substring(12).split(","))
                                .map(s -> s.replace("'", "").trim())
                                .collect(Collectors.toList());

                String clusterName = infos.get(0);
                String localDatabase = infos.get(1);
                String localTable = infos.get(2).replace(")", "").trim();

                String localTableSQL =
                        String.format(
                                "select engine,create_table_query from system.tables where database = '%s' and name = '%s'",
                                localDatabase, localTable);
                String localTableDDL;
                String localTableEngine;
                try (ProtonResponse localTableResponse =
                        ProtonRequest.query(localTableSQL).executeAndWait()) {
                    List<ProtonRecord> localTableRecords =
                            localTableResponse.stream().collect(Collectors.toList());
                    if (localTableRecords.isEmpty()) {
                        throw new TimeplusConnectorException(
                                SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED,
                                "Cannot get table from Timeplus, resultSet is empty");
                    }
                    localTableEngine = localTableRecords.get(0).getValue(0).asString();
                    localTableDDL = localTableRecords.get(0).getValue(1).asString();
                    localTableDDL = localizationEngine(localTableEngine, localTableDDL);
                }

                return new DistributedEngine(
                        clusterName, localDatabase, localTable, localTableEngine, localTableDDL);
            }
            throw new TimeplusConnectorException(
                    SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED,
                    "Cannot get distributed table from Timeplus, resultSet is empty");
        } catch (ProtonException e) {
            throw new TimeplusConnectorException(
                    SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED,
                    "Cannot get distributed table from Timeplus",
                    e);
        }
    }

    /**
     * Get Timeplus table schema, the key is fileName, value is value type.
     *
     * @param table table name.
     * @return schema map.
     */
    public Map<String, String> getTimeplusTableSchema(String table) {
        ProtonRequest<?> request = getProtonConnection();
        return getTimeplusTableSchema(request, table);
    }

    public Map<String, String> getTimeplusTableSchema(ProtonRequest<?> request, String table) {
        String sql = "desc " + table;
        Map<String, String> schema = new LinkedHashMap<>();
        try (ProtonResponse response = request.query(sql).executeAndWait()) {
            response.records()
                    .forEach(
                            r -> {
                                if (!"MATERIALIZED".equals(r.getValue(2).asString())) {
                                    schema.put(r.getValue(0).asString(), r.getValue(1).asString());
                                }
                            });
        } catch (ProtonException e) {
            throw new TimeplusConnectorException(
                    CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED,
                    "Cannot get schema from Timeplus",
                    e);
        }
        return schema;
    }

    /**
     * Get the shard of the given cluster.
     *
     * @param connection Timeplus connection.
     * @param clusterName cluster name.
     * @param database database of the shard.
     * @param port port of the shard.
     * @return shard list.
     */
    public List<Shard> getClusterShardList(
            ProtonRequest<?> connection,
            String clusterName,
            String database,
            int port,
            String username,
            String password) {
        String sql =
                "select shard_num,shard_weight,replica_num,host_name,host_address,port from system.clusters where cluster = '"
                        + clusterName
                        + "'"
                        + " and replica_num=1";
        List<Shard> shardList = new ArrayList<>();
        try (ProtonResponse response = connection.query(sql).executeAndWait()) {
            response.records()
                    .forEach(
                            r -> {
                                shardList.add(
                                        new Shard(
                                                r.getValue(0).asInteger(),
                                                r.getValue(1).asInteger(),
                                                r.getValue(2).asInteger(),
                                                r.getValue(3).asString(),
                                                r.getValue(4).asString(),
                                                port,
                                                database,
                                                username,
                                                password));
                            });
            return shardList;
        } catch (ProtonException e) {
            throw new TimeplusConnectorException(
                    TimeplusConnectorErrorCode.CLUSTER_LIST_GET_FAILED,
                    "Cannot get cluster shard list from Timeplus",
                    e);
        }
    }

    /**
     * Get Timeplus table info.
     *
     * @param database database of the table.
     * @param table table name of the table.
     * @return Timeplus table info.
     */
    public TimeplusTable getTimeplusTable(String database, String table) {
        String sql =
                String.format(
                        "select engine,create_table_query,engine_full,data_paths,sorting_key from system.tables where database = '%s' and name = '%s'",
                        database, table);
        try (ProtonResponse response = ProtonRequest.query(sql).executeAndWait()) {
            List<ProtonRecord> records = response.stream().collect(Collectors.toList());
            if (records.isEmpty()) {
                throw new TimeplusConnectorException(
                        SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED,
                        "Cannot get table from Timeplus, resultSet is empty");
            }
            ProtonRecord record = records.get(0);
            String engine = record.getValue(0).asString();
            String createTableDDL = record.getValue(1).asString();
            String engineFull = record.getValue(2).asString();
            List<String> dataPaths =
                    record.getValue(3).asTuple().stream()
                            .map(Object::toString)
                            .collect(Collectors.toList());
            String sortingKey = record.getValue(4).asString();
            DistributedEngine distributedEngine = null;
            if ("Distributed".equals(engine)) {
                distributedEngine = getProtonDistributedTable(ProtonRequest, database, table);
                createTableDDL = distributedEngine.getTableDDL();
            }
            return new TimeplusTable(
                    database,
                    table,
                    distributedEngine,
                    engine,
                    createTableDDL,
                    engineFull,
                    dataPaths,
                    sortingKey,
                    getTimeplusTableSchema(ProtonRequest, table));
        } catch (ProtonException e) {
            throw new TimeplusConnectorException(
                    SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED, "Cannot get Timeplus table", e);
        }
    }

    /**
     * Localization the engine in Timeplus local table's createTableDDL to support specific engine.
     * For example: change ReplicatedMergeTree to MergeTree.
     *
     * @param engine original engine of Timeplus local table
     * @param ddl createTableDDL of Timeplus local table
     * @return createTableDDL of Timeplus local table which can support specific engine TODO:
     *     support more engine
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
        shardToDataSource.values().forEach(ProtonClient::close);
    }
}
