/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class HbaseCluster {

    private static final Logger LOG = LoggerFactory.getLogger(HbaseCluster.class);

    private static final DockerImageName HBASE_DOCKER_IMAGE =
            DockerImageName.parse("jcjabouille/hbase-standalone:2.4.9");

    private HbaseContainer hbaseContainer;
    private Connection connection;

    public Connection startService() throws IOException {
        hbaseContainer = new HbaseContainer(HBASE_DOCKER_IMAGE);
        hbaseContainer.start();
        hbaseContainer.waitingFor(Wait.defaultWaitStrategy());
        String zookeeperQuorum = hbaseContainer.getZookeeperQuorum();
        LOG.info("Successfully start hbase service, zookeeper quorum: {}", zookeeperQuorum);
        Configuration configuration = hbaseContainer.getConfiguration();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        connection = ConnectionFactory.createConnection(configuration);

        return connection;
    }

    public void createTable(String tableName, List<String> list) throws IOException {
        TableDescriptorBuilder tableDesc =
                TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

        List<ColumnFamilyDescriptor> colFamilyList = new ArrayList<>();
        for (String columnFamilys : list) {
            ColumnFamilyDescriptorBuilder c =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamilys));
            colFamilyList.add(c.build());
        }
        tableDesc.setColumnFamilies(colFamilyList);
        Admin hbaseAdmin = connection.getAdmin();
        hbaseAdmin.createTable(tableDesc.build());
    }

    public void putRow(
            String tableName,
            String rowKey,
            String columnFamilyName,
            String qualifier,
            String value)
            throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(
                Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    public String getCell(String tableName, String rowKey, String columnFamily, String qualifier)
            throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            Result result = table.get(get);
            byte[] resultValue =
                    result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            return Bytes.toString(resultValue);
        } else {
            return null;
        }
    }

    public void stopService() throws IOException {
        if (Objects.nonNull(connection)) {
            connection.close();
        }
        if (Objects.nonNull(hbaseContainer)) {
            hbaseContainer.close();
        }
        hbaseContainer = null;
    }
}
