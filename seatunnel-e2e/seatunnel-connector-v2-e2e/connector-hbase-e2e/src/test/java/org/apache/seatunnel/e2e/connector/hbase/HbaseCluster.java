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

import org.apache.seatunnel.e2e.common.container.ContainerTcpProxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.alibaba.dcm.DnsCacheManipulator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.seatunnel.e2e.common.container.TestContainer.NETWORK;

public class HbaseCluster {

    private static final Logger LOG = LoggerFactory.getLogger(HbaseCluster.class);

    private static final int ZOOKEEPER_PORT = 2181;
    private static final int MASTER_PORT = 16000;
    private static final int REGION_PORT = 16020;
    private static final String HOST = "hbase_e2e";

    private static final String DOCKER_NAME = "seatunnelhub/hbase-standalone:2.4.9";
    private static final DockerImageName HBASE_DOCKER_IMAGE = DockerImageName.parse(DOCKER_NAME);

    private Connection connection;
    private GenericContainer<?> hbaseContainer;
    private ContainerTcpProxy proxy;

    public Connection startService() throws IOException {
        hbaseContainer =
                new GenericContainer<>(HBASE_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withExposedPorts(MASTER_PORT, REGION_PORT, ZOOKEEPER_PORT)
                        .withCreateContainerCmdModifier(cmd -> cmd.withHostName(HOST))
                        .withEnv("HBASE_MASTER_PORT", String.valueOf(MASTER_PORT))
                        .withEnv("HBASE_REGION_PORT", String.valueOf(REGION_PORT))
                        .withEnv(
                                "HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT",
                                String.valueOf(ZOOKEEPER_PORT))
                        .withEnv("HBASE_ZOOKEEPER_QUORUM", HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_NAME)));
        Startables.deepStart(Stream.of(hbaseContainer)).join();

        proxy =
                ContainerTcpProxy.start(
                        Arrays.asList(
                                ContainerTcpProxy.PortMapping.of(
                                        MASTER_PORT,
                                        hbaseContainer.getHost(),
                                        hbaseContainer.getMappedPort(MASTER_PORT)),
                                ContainerTcpProxy.PortMapping.of(
                                        REGION_PORT,
                                        hbaseContainer.getHost(),
                                        hbaseContainer.getMappedPort(REGION_PORT)),
                                ContainerTcpProxy.PortMapping.of(
                                        ZOOKEEPER_PORT,
                                        hbaseContainer.getHost(),
                                        hbaseContainer.getMappedPort(ZOOKEEPER_PORT))));
        DnsCacheManipulator.setDnsCache(HOST, proxy.getLoopbackAddress());
        LOG.info("HBase container started");

        String zookeeperQuorum = getZookeeperQuorum();
        LOG.info("Successfully start hbase service, zookeeper quorum: {}", zookeeperQuorum);
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.security.authentication", "simple");
        configuration.set("hbase.rpc.timeout", "10000");
        configuration.set("hbase.master.port", String.valueOf(MASTER_PORT));
        configuration.set("hbase.regionserver.port", String.valueOf(REGION_PORT));
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

    public void stopService() throws IOException {
        if (Objects.nonNull(connection)) {
            connection.close();
        }
        DnsCacheManipulator.removeDnsCache(HOST);
        if (Objects.nonNull(proxy)) {
            proxy.close();
        }
        if (Objects.nonNull(hbaseContainer)) {
            hbaseContainer.close();
        }
        proxy = null;
        hbaseContainer = null;
    }

    public static String getZookeeperQuorum() {
        return String.format("%s:%s", HOST, ZOOKEEPER_PORT);
    }
}
