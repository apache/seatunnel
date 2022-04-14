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

package org.apache.seatunnel.flink.clickhouse.sink.client;

import static org.apache.seatunnel.flink.clickhouse.ConfigKey.CLICKHOUSE_PREFIX;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.DATABASE;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.HOST;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.PASSWORD;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.USERNAME;

import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.flink.clickhouse.pojo.DistributedEngine;
import org.apache.seatunnel.flink.clickhouse.pojo.Shard;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHouseStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@SuppressWarnings("magicnumber")
public class ClickhouseClient {

    private final BalancedClickhouseDataSource balancedClickhouseDataSource;

    public ClickhouseClient(Config config) {
        Properties clickhouseProperties = new Properties();
        if (TypesafeConfigUtils.hasSubConfig(config, CLICKHOUSE_PREFIX)) {
            TypesafeConfigUtils.extractSubConfig(config, CLICKHOUSE_PREFIX, false).entrySet().forEach(e -> {
                clickhouseProperties.put(e.getKey(), String.valueOf(e.getValue().unwrapped()));
            });
        }
        clickhouseProperties.put("user", config.getString(USERNAME));
        clickhouseProperties.put("password", config.getString(PASSWORD));
        String jdbcUrl = "jdbc:clickhouse://" + config.getString(HOST) + "/" + config.getString(DATABASE);
        this.balancedClickhouseDataSource = new BalancedClickhouseDataSource(jdbcUrl, clickhouseProperties);
    }

    public ClickHouseConnectionImpl getClickhouseConnection() {
        try {
            return (ClickHouseConnectionImpl) balancedClickhouseDataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Cannot connect to clickhouse server", e);
        }
    }

    public DistributedEngine getClickhouseDistributedTable(String database, String table) {
        try (ClickHouseConnection connection = getClickhouseConnection()) {
            return getClickhouseDistributedTable(connection, database, table);
        } catch (SQLException e) {
            throw new RuntimeException("Cannot get distributed table from clickhouse", e);
        }
    }

    public DistributedEngine getClickhouseDistributedTable(ClickHouseConnection connection, String database, String table) {
        String sql = String.format("select engine_full from system.tables where database = '%s' and name = '%s' and engine = 'Distributed'", database, table);
        try (ClickHouseStatement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                // engineFull field will be like : Distributed(cluster, database, table[, sharding_key[, policy_name]])
                String engineFull = resultSet.getString(1);
                List<String> infos = Arrays.stream(engineFull.substring(12).split(","))
                    .map(s -> s.replace("'", "").trim()).collect(Collectors.toList());
                return new DistributedEngine(infos.get(0), infos.get(1), infos.get(2).replace("\\)", "").trim());
            }
            throw new RuntimeException("Cannot get distributed table from clickhouse, resultSet is empty");
        } catch (SQLException e) {
            throw new RuntimeException("Cannot get distributed table from clickhouse", e);
        }
    }

    public Map<String, String> getClickhouseTableSchema(String table) {
        try (ClickHouseConnection connection = getClickhouseConnection()) {
            return getClickhouseTableSchema(connection, table);
        } catch (SQLException e) {
            throw new RuntimeException("Cannot get table schema from clickhouse", e);
        }
    }

    public Map<String, String> getClickhouseTableSchema(ClickHouseConnection connection, String table) {
        String sql = "desc " + table;
        Map<String, String> schema = new LinkedHashMap<>();
        try (ClickHouseStatement clickHouseStatement = connection.createStatement()) {
            ResultSet resultSet = clickHouseStatement.executeQuery(sql);
            while (resultSet.next()) {
                schema.put(resultSet.getString(1), resultSet.getString(2));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot get table schema from clickhouse", e);
        }
        return schema;
    }

    public List<Shard> getClusterShardList(ClickHouseConnection connection, String clusterName, String database, String port) {
        String sql = "select shard_num,shard_weight,replica_num,host_name,host_address,port from system.clusters where cluster = '" + clusterName + "'";
        List<Shard> shardList = new ArrayList<>();
        try (ClickHouseStatement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                shardList.add(new Shard(
                    resultSet.getInt(1),
                    resultSet.getInt(2),
                    resultSet.getInt(3),
                    resultSet.getString(4),
                    resultSet.getString(5),
                    port,
                    database));
            }
            return shardList;
        } catch (SQLException e) {
            throw new RuntimeException("Cannot get cluster shard list from clickhouse", e);
        }
    }

}
