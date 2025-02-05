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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.util;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseTable;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
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
        ClickHouseClient c =
                shardToDataSource.computeIfAbsent(
                        shard, s -> ClickHouseClient.newInstance(s.getNode().getProtocol()));
        return c.connect(shard.getNode()).format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
    }

    public DistributedEngine getClickhouseDistributedTable(
            ClickHouseRequest<?> connection, String database, String table) {
        String sql =
                String.format(
                        "select engine_full from system.tables where database = '%s' and name = '%s' and engine = 'Distributed'",
                        database, table);
        try (ClickHouseResponse response = connection.query(sql).executeAndWait()) {
            List<ClickHouseRecord> records = response.stream().collect(Collectors.toList());
            if (!records.isEmpty()) {
                ClickHouseRecord record = records.get(0);
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
                try (ClickHouseResponse localTableResponse =
                        clickhouseRequest.query(localTableSQL).executeAndWait()) {
                    List<ClickHouseRecord> localTableRecords =
                            localTableResponse.stream().collect(Collectors.toList());
                    if (localTableRecords.isEmpty()) {
                        throw new ClickhouseConnectorException(
                                SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED,
                                "Cannot get table from clickhouse, resultSet is empty");
                    }
                    localTableEngine = localTableRecords.get(0).getValue(0).asString();
                    localTableDDL = localTableRecords.get(0).getValue(1).asString();
                    localTableDDL = localizationEngine(localTableEngine, localTableDDL);
                }

                return new DistributedEngine(
                        clusterName, localDatabase, localTable, localTableEngine, localTableDDL);
            }
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED,
                    "Cannot get distributed table from clickhouse, resultSet is empty");
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED,
                    "Cannot get distributed table from clickhouse",
                    e);
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

    public Map<String, String> getClickhouseTableSchema(
            ClickHouseRequest<?> request, String table) {
        String sql = "desc " + table;
        Map<String, String> schema = new LinkedHashMap<>();
        try (ClickHouseResponse response = request.query(sql).executeAndWait()) {
            response.records()
                    .forEach(
                            r -> {
                                if (!"MATERIALIZED".equals(r.getValue(2).asString())) {
                                    schema.put(r.getValue(0).asString(), r.getValue(1).asString());
                                }
                            });
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED,
                    "Cannot get table schema from clickhouse",
                    e);
        }
        return schema;
    }

    public List<ClickHouseColumn> getClickHouseColumns(String table) {
        String sql = "desc " + table;
        try (ClickHouseResponse response = this.clickhouseRequest.query(sql).executeAndWait()) {
            return response.getColumns();

        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED,
                    "Cannot get table schema from clickhouse",
                    e);
        }
    }

    /**
     * Get the shard of the given cluster.
     *
     * @param connection clickhouse connection.
     * @param clusterName cluster name.
     * @param database database of the shard.
     * @param port port of the shard.
     * @return shard list.
     */
    public List<Shard> getClusterShardList(
            ClickHouseRequest<?> connection,
            String clusterName,
            String database,
            int port,
            String username,
            String password,
            Map<String, String> options) {
        String sql =
                "select shard_num,shard_weight,replica_num,host_name,host_address,port from system.clusters where cluster = '"
                        + clusterName
                        + "'"
                        + " and replica_num=1";
        List<Shard> shardList = new ArrayList<>();
        try (ClickHouseResponse response = connection.query(sql).executeAndWait()) {
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
                                                password,
                                                options));
                            });
            return shardList;
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.CLUSTER_LIST_GET_FAILED,
                    "Cannot get cluster shard list from clickhouse",
                    e);
        }
    }

    /**
     * Get ClickHouse table info.
     *
     * @param database database of the table.
     * @param table table name of the table.
     * @return clickhouse table info.
     */
    public ClickhouseTable getClickhouseTable(
            ClickHouseRequest<?> clickhouseRequest, String database, String table) {
        String sql =
                String.format(
                        "select engine,create_table_query,engine_full,data_paths,sorting_key from system.tables where database = '%s' and name = '%s'",
                        database, table);
        try (ClickHouseResponse response = clickhouseRequest.query(sql).executeAndWait()) {
            List<ClickHouseRecord> records = response.stream().collect(Collectors.toList());
            if (records.isEmpty()) {
                throw new ClickhouseConnectorException(
                        SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED,
                        "Cannot get table from clickhouse, resultSet is empty");
            }
            ClickHouseRecord record = records.get(0);
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
                distributedEngine =
                        getClickhouseDistributedTable(clickhouseRequest, database, table);
                createTableDDL = distributedEngine.getTableDDL();
            }
            return new ClickhouseTable(
                    database,
                    table,
                    distributedEngine,
                    engine,
                    createTableDDL,
                    engineFull,
                    dataPaths,
                    sortingKey,
                    getClickhouseTableSchema(clickhouseRequest, table));
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED, "Cannot get clickhouse table", e);
        }
    }

    /**
     * Localization the engine in clickhouse local table's createTableDDL to support specific
     * engine. For example: change ReplicatedMergeTree to MergeTree.
     *
     * @param engine original engine of clickhouse local table
     * @param ddl createTableDDL of clickhouse local table
     * @return createTableDDL of clickhouse local table which can support specific engine TODO:
     *     support more engine
     */
    public String localizationEngine(String engine, String ddl) {
        if ("ReplicatedMergeTree".equalsIgnoreCase(engine)) {
            return ddl.replaceAll("ReplicatedMergeTree(\\([^\\)]*\\))", "MergeTree()");
        } else {
            return ddl;
        }
    }

    public boolean tableExists(String database, String table) {
        String sql =
                String.format(
                        "select count(1) from system.tables where database = '%s' and name = '%s'",
                        database, table);
        try (ClickHouseResponse response = clickhouseRequest.query(sql).executeAndWait()) {
            return response.firstRecord().getValue(0).asInteger() > 0;
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED, "Cannot get table from clickhouse", e);
        }
    }

    public List<String> listDatabases() {
        String sql = "select distinct database from system.tables";
        try (ClickHouseResponse response = clickhouseRequest.query(sql).executeAndWait()) {
            Iterable<ClickHouseRecord> records = response.records();
            return StreamSupport.stream(records.spliterator(), false)
                    .map(r -> r.getValue(0).asString())
                    .collect(Collectors.toList());
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.LIST_DATABASES_FAILED,
                    "Cannot list databases from clickhouse",
                    e);
        }
    }

    public List<String> listTable(String database) {
        String sql = "SELECT name FROM system.tables WHERE database = '" + database + "'";
        try (ClickHouseResponse response = clickhouseRequest.query(sql).executeAndWait()) {
            Iterable<ClickHouseRecord> records = response.records();
            return StreamSupport.stream(records.spliterator(), false)
                    .map(r -> r.getValue(0).asString())
                    .collect(Collectors.toList());
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.LIST_TABLES_FAILED,
                    "Cannot list tables from clickhouse",
                    e);
        }
    }

    public void executeSql(String sql) {
        try {
            clickhouseRequest
                    .write()
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query(sql)
                    .execute()
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void createTable(
            String database,
            String table,
            String template,
            String comment,
            TableSchema tableSchema) {
        String createTableSql =
                ClickhouseCatalogUtil.INSTANCE.getCreateTableSql(
                        template,
                        database,
                        table,
                        tableSchema,
                        comment,
                        ClickhouseConfig.SAVE_MODE_CREATE_TEMPLATE.key());
        log.debug("Create Clickhouse table sql: {}", createTableSql);
        executeSql(createTableSql);
    }

    public Optional<PrimaryKey> getPrimaryKey(String schema, String table) throws SQLException {

        List<String> pkFields;
        String sql =
                "SELECT\n"
                        + "    name as column_name\n"
                        + "FROM system.columns\n"
                        + "WHERE table = '"
                        + table
                        + "'\n"
                        + "  AND database = '"
                        + schema
                        + "'\n"
                        + "  AND is_in_primary_key = 1\n"
                        + "ORDER BY position;";
        try (ClickHouseResponse response = clickhouseRequest.query(sql).executeAndWait()) {
            Iterable<ClickHouseRecord> records = response.records();
            pkFields =
                    StreamSupport.stream(records.spliterator(), false)
                            .map(r -> r.getValue(0).asString())
                            .collect(Collectors.toList());
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.GET_PRIMARY_KEY_FAILED,
                    "Cannot get primary key from clickhouse",
                    e);
        }
        if (!pkFields.isEmpty()) {
            // PK_NAME maybe null according to the javadoc, generate a unique name in that case
            String pkName = "pk_" + String.join("_", pkFields);
            return Optional.of(PrimaryKey.of(pkName, pkFields));
        }
        return Optional.empty();
    }

    public boolean isExistsData(String tableName) throws ExecutionException, InterruptedException {
        String queryDataSql = "SELECT count(*) FROM " + tableName;
        try (ClickHouseResponse response = clickhouseRequest.query(queryDataSql).executeAndWait()) {
            return response.firstRecord().getValue(0).asInteger() > 0;
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED, "Cannot get table from clickhouse", e);
        }
    }

    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists) {
        executeSql(ClickhouseCatalogUtil.INSTANCE.getDropTableSql(tablePath, ignoreIfNotExists));
    }

    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists) {
        executeSql(ClickhouseCatalogUtil.INSTANCE.getTruncateTableSql(tablePath));
    }

    public void createDatabase(String database, boolean ignoreIfExists) {
        executeSql(ClickhouseCatalogUtil.INSTANCE.getCreateDatabaseSql(database, ignoreIfExists));
    }

    public void dropDatabase(String database, boolean ignoreIfNotExists) {
        executeSql(ClickhouseCatalogUtil.INSTANCE.getDropDatabaseSql(database, ignoreIfNotExists));
    }

    public void close() {
        if (this.client != null) {
            this.client.close();
        }
        shardToDataSource.values().forEach(ClickHouseClient::close);
    }
}
