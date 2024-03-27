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

package org.apache.seatunnel.e2e.connector.hbase;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Stream;

@Slf4j
@Disabled(
        "Hbase docker e2e case need user add mapping information of between container id and ip address in hosts file")
public class HbaseIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "harisekhon/hbase:latest";

    private static final int PORT = 2181;

    private static final String HOST = "hbase-e2e";

    private static final String TABLE_NAME = "seatunnel_test";

    private static final String FAMILY_NAME = "info";

    private final Configuration hbaseConfiguration = HBaseConfiguration.create();

    private Connection hbaseConnection;

    private Admin admin;

    private TableName table;

    private GenericContainer<?> hbaseContainer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        hbaseContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withExposedPorts(PORT)
                        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        Startables.deepStart(Stream.of(hbaseContainer)).join();
        log.info("Hbase container started");
        this.initialize();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (Objects.nonNull(admin)) {
            admin.close();
        }
        if (Objects.nonNull(hbaseConnection)) {
            hbaseConnection.close();
        }
        if (Objects.nonNull(hbaseContainer)) {
            hbaseContainer.close();
        }
    }

    private void initialize() throws IOException {
        hbaseConfiguration.set("hbase.zookeeper.quorum", HOST + ":" + PORT);
        hbaseConnection = ConnectionFactory.createConnection(hbaseConfiguration);
        admin = hbaseConnection.getAdmin();
        table = TableName.valueOf(TABLE_NAME);
        ColumnFamilyDescriptor familyDescriptor =
                ColumnFamilyDescriptorBuilder.newBuilder(FAMILY_NAME.getBytes())
                        .setCompressionType(Compression.Algorithm.SNAPPY)
                        .setCompactionCompressionType(Compression.Algorithm.SNAPPY)
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptorBuilder.newBuilder(table).setColumnFamily(familyDescriptor).build();
        admin.createTable(tableDescriptor);
        log.info("Hbase table has been initialized");
    }

    @TestTemplate
    public void testHbaseSink(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/fake-to-hbase.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Table hbaseTable = hbaseConnection.getTable(table);
        Scan scan = new Scan();
        ArrayList<Result> results = new ArrayList<>();
        ResultScanner scanner = hbaseTable.getScanner(scan);
        for (Result result : scanner) {
            results.add(result);
        }
        Assertions.assertEquals(results.size(), 5);
    }

    @TestTemplate
    public void testHbaseSource(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/hbase-to-assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testHbaseSinkWithArray(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/fake-to-hbase-array.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Table hbaseTable = hbaseConnection.getTable(table);
        Scan scan = new Scan();
        ArrayList<Result> results = new ArrayList<>();
        ResultScanner scanner = hbaseTable.getScanner(scan);
        for (Result result : scanner) {
            String rowKey = Bytes.toString(result.getRow());
            for (Cell cell : result.listCells()) {
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                if ("A".equals(rowKey) && "info:c_array_string".equals(columnName)) {
                    Assertions.assertEquals(value, "\"a\",\"b\",\"c\"");
                }
                if ("B".equals(rowKey) && "info:c_array_int".equals(columnName)) {
                    Assertions.assertEquals(value, "4,5,6");
                }
            }
            results.add(result);
        }
        Assertions.assertEquals(results.size(), 3);
    }
}
