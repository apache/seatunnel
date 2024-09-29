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

package org.apache.seatunnel.connectors.seatunnel.hbase.client;

import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hbase.source.HbaseSourceSplit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorErrorCode.CONNECTION_FAILED_FOR_ADMIN;

@Slf4j
public class HbaseClient {

    private final Connection connection;
    private final Admin admin;
    private final BufferedMutator hbaseMutator;
    public static Configuration hbaseConfiguration;

    /**
     * Constructor for HbaseClient.
     *
     * @param connection Hbase connection
     * @param hbaseParameters Hbase parameters
     */
    private HbaseClient(Connection connection, HbaseParameters hbaseParameters) {
        this.connection = connection;
        try {
            this.admin = connection.getAdmin();

            BufferedMutatorParams bufferedMutatorParams =
                    new BufferedMutatorParams(
                                    TableName.valueOf(
                                            hbaseParameters.getNamespace(),
                                            hbaseParameters.getTable()))
                            .pool(HTable.getDefaultExecutor(hbaseConfiguration))
                            .writeBufferSize(hbaseParameters.getWriteBufferSize());
            hbaseMutator = connection.getBufferedMutator(bufferedMutatorParams);
        } catch (IOException e) {
            throw new HbaseConnectorException(
                    CONNECTION_FAILED_FOR_ADMIN, CONNECTION_FAILED_FOR_ADMIN.getDescription(), e);
        }
    }

    /**
     * Create a new instance of HbaseClient.
     *
     * @param hbaseParameters Hbase parameters
     * @return HbaseClient
     */
    public static HbaseClient createInstance(HbaseParameters hbaseParameters) {
        return new HbaseClient(getHbaseConnection(hbaseParameters), hbaseParameters);
    }

    /**
     * Get Hbase connection.
     *
     * @param hbaseParameters Hbase parameters
     * @return Hbase connection
     */
    private static Connection getHbaseConnection(HbaseParameters hbaseParameters) {
        hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.set("hbase.zookeeper.quorum", hbaseParameters.getZookeeperQuorum());
        if (hbaseParameters.getHbaseExtraConfig() != null) {
            hbaseParameters.getHbaseExtraConfig().forEach(hbaseConfiguration::set);
        }
        try {
            Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
            return connection;
        } catch (IOException e) {
            String errorMsg = "Build Hbase connection failed.";
            throw new HbaseConnectorException(
                    HbaseConnectorErrorCode.CONNECTION_FAILED, errorMsg, e);
        }
    }

    /**
     * Check if a database exists.
     *
     * @param databaseName database name
     * @return true if the database exists, false otherwise
     */
    public boolean databaseExists(String databaseName) {
        try {
            return Arrays.stream(admin.listNamespaceDescriptors())
                    .anyMatch(descriptor -> descriptor.getName().equals(databaseName));
        } catch (IOException e) {
            throw new HbaseConnectorException(
                    HbaseConnectorErrorCode.DATABASE_QUERY_EXCEPTION,
                    HbaseConnectorErrorCode.DATABASE_QUERY_EXCEPTION.getErrorMessage(),
                    e);
        }
    }

    /**
     * List all databases.
     *
     * @return List of database names
     */
    public List<String> listDatabases() {
        try {
            return Arrays.stream(admin.listNamespaceDescriptors())
                    .map(NamespaceDescriptor::getName)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new HbaseConnectorException(
                    HbaseConnectorErrorCode.DATABASE_QUERY_EXCEPTION,
                    HbaseConnectorErrorCode.DATABASE_QUERY_EXCEPTION.getErrorMessage(),
                    e);
        }
    }

    /**
     * List all tables in a database.
     *
     * @param databaseName database name
     * @return List of table names
     */
    public List<String> listTables(String databaseName) {
        try {
            return Arrays.stream(admin.listTableNamesByNamespace(databaseName))
                    .map(tableName -> tableName.getNameAsString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new HbaseConnectorException(
                    HbaseConnectorErrorCode.DATABASE_QUERY_EXCEPTION,
                    HbaseConnectorErrorCode.DATABASE_QUERY_EXCEPTION.getErrorMessage(),
                    e);
        }
    }

    /**
     * Check if a table exists.
     *
     * @param tableName table name
     * @return true if the table exists, false otherwise
     */
    public boolean tableExists(String tableName) {
        try {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new HbaseConnectorException(
                    HbaseConnectorErrorCode.TABLE_QUERY_EXCEPTION,
                    HbaseConnectorErrorCode.TABLE_QUERY_EXCEPTION.getErrorMessage(),
                    e);
        }
    }

    /**
     * Create a table.
     *
     * @param databaseName database name
     * @param tableName table name
     * @param columnFamilies column families
     * @param ignoreIfExists ignore if the table already exists
     */
    public void createTable(
            String databaseName,
            String tableName,
            List<String> columnFamilies,
            boolean ignoreIfExists) {
        try {
            if (!databaseExists(databaseName) && !StringUtils.isBlank(databaseName)) {
                admin.createNamespace(NamespaceDescriptor.create(databaseName).build());
            }
            TableName table = TableName.valueOf(databaseName, tableName);
            if (tableExists(table.getNameAsString())) {
                log.info("Table {} already exists.", table.getNameAsString());
                if (!ignoreIfExists) {
                    throw new HbaseConnectorException(
                            HbaseConnectorErrorCode.TABLE_EXISTS_EXCEPTION,
                            HbaseConnectorErrorCode.TABLE_EXISTS_EXCEPTION.getErrorMessage());
                }
                return;
            }
            TableDescriptorBuilder hbaseTableDescriptor = TableDescriptorBuilder.newBuilder(table);
            columnFamilies.forEach(
                    family ->
                            hbaseTableDescriptor.setColumnFamily(
                                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family))
                                            .build()));
            admin.createTable(hbaseTableDescriptor.build());
        } catch (IOException e) {
            throw new HbaseConnectorException(
                    HbaseConnectorErrorCode.TABLE_CREATE_EXCEPTION,
                    HbaseConnectorErrorCode.TABLE_CREATE_EXCEPTION.getErrorMessage(),
                    e);
        }
    }

    /**
     * Drop a table.
     *
     * @param databaseName database name
     * @param tableName table name
     */
    public void dropTable(String databaseName, String tableName) {
        try {
            TableName table = TableName.valueOf(databaseName, tableName);
            admin.disableTable(table);
            admin.deleteTable(table);
        } catch (IOException e) {
            throw new HbaseConnectorException(
                    HbaseConnectorErrorCode.TABLE_DELETE_EXCEPTION,
                    HbaseConnectorErrorCode.TABLE_DELETE_EXCEPTION.getErrorMessage(),
                    e);
        }
    }

    /**
     * Create a namespace.
     *
     * @param namespace namespace name
     */
    public void createNamespace(String namespace) {
        try {
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        } catch (IOException e) {
            throw new HbaseConnectorException(
                    HbaseConnectorErrorCode.NAMESPACE_CREATE_EXCEPTION,
                    HbaseConnectorErrorCode.NAMESPACE_CREATE_EXCEPTION.getErrorMessage(),
                    e);
        }
    }

    /**
     * Drop a namespace.
     *
     * @param namespace namespace name
     */
    public void deleteNamespace(String namespace) {
        try {
            admin.deleteNamespace(namespace);
        } catch (IOException e) {
            throw new HbaseConnectorException(
                    HbaseConnectorErrorCode.NAMESPACE_DELETE_EXCEPTION,
                    HbaseConnectorErrorCode.NAMESPACE_DELETE_EXCEPTION.getErrorMessage(),
                    e);
        }
    }

    /**
     * Truncate a table.
     *
     * @param databaseName database name
     * @param tableName table name
     */
    public void truncateTable(String databaseName, String tableName) {
        try {
            TableName table = TableName.valueOf(databaseName, tableName);
            admin.disableTable(table);
            admin.truncateTable(table, true);
        } catch (IOException e) {
            throw new HbaseConnectorException(
                    HbaseConnectorErrorCode.TABLE_TRUNCATE_EXCEPTION,
                    HbaseConnectorErrorCode.TABLE_TRUNCATE_EXCEPTION.getErrorMessage(),
                    e);
        }
    }

    /**
     * Check if a table has data.
     *
     * @param databaseName database name
     * @param tableName table name
     * @return true if the table has data, false otherwise
     */
    public boolean isExistsData(String databaseName, String tableName) {
        try {
            Table table = connection.getTable(TableName.valueOf(databaseName, tableName));
            Scan scan = new Scan();
            scan.setCaching(1);
            scan.setLimit(1);
            try (ResultScanner scanner = table.getScanner(scan)) {
                Result result = scanner.next();
                return !result.isEmpty();
            }
        } catch (IOException e) {
            throw new HbaseConnectorException(
                    HbaseConnectorErrorCode.TABLE_QUERY_EXCEPTION,
                    HbaseConnectorErrorCode.TABLE_QUERY_EXCEPTION.getErrorMessage(),
                    e);
        }
    }

    /** Close Hbase connection. */
    public void close() {
        try {
            if (hbaseMutator != null) {
                hbaseMutator.flush();
                hbaseMutator.close();
            }
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            log.error("Close Hbase connection failed.", e);
        }
    }

    /**
     * Mutate a Put.
     *
     * @param put Hbase put
     * @throws IOException exception
     */
    public void mutate(Put put) throws IOException {
        hbaseMutator.mutate(put);
    }

    /**
     * Scan a table.
     *
     * @param split Hbase source split
     * @param hbaseParameters Hbase parameters
     * @param columnNames column names
     * @return ResultScanner
     * @throws IOException exception
     */
    public ResultScanner scan(
            HbaseSourceSplit split, HbaseParameters hbaseParameters, List<String> columnNames)
            throws IOException {
        Scan scan = new Scan();
        scan.withStartRow(split.getStartRow(), true);
        scan.withStopRow(split.getEndRow(), true);
        scan.setCacheBlocks(hbaseParameters.isCacheBlocks());
        scan.setCaching(hbaseParameters.getCaching());
        scan.setBatch(hbaseParameters.getBatch());
        for (String columnName : columnNames) {
            String[] columnNameSplit = columnName.split(":");
            scan.addColumn(Bytes.toBytes(columnNameSplit[0]), Bytes.toBytes(columnNameSplit[1]));
        }
        return this.connection
                .getTable(TableName.valueOf(hbaseParameters.getTable()))
                .getScanner(scan);
    }

    /**
     * Get a RegionLocator.
     *
     * @param tableName table name
     * @return RegionLocator
     * @throws IOException exception
     */
    public RegionLocator getRegionLocator(String tableName) throws IOException {
        return this.connection.getRegionLocator(TableName.valueOf(tableName));
    }
}
