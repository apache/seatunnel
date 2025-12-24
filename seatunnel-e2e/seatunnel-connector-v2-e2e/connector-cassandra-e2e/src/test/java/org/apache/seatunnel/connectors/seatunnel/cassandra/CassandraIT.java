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

package org.apache.seatunnel.connectors.seatunnel.cassandra;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Pair;
import org.testcontainers.utility.DockerLoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class CassandraIT extends TestSuiteBase implements TestResource {
    private static final String CASSANDRA_DOCKER_IMAGE = "cassandra:4.1.1";
    private static final String HOST = "cassandra";
    private static final Integer PORT = 9042;
    private static final String INIT_CASSANDRA_PATH = "/init/cassandra_init.conf";
    private static final String CASSANDRA_JOB_CONFIG = "/cassandra_to_cassandra.conf";
    private static final String CASSANDRA_DRIVER_CONFIG = "/application.conf";
    private static final String DATACENTER = "datacenter1";
    private static final String KEYSPACE = "test";
    private static final String SOURCE_TABLE = "source_table";
    private static final String SINK_TABLE = "sink_table";
    private static final String INSERT_CQL = "insert_cql";
    private static final Pair<SeaTunnelRowType, List<SeaTunnelRow>> TEST_DATASET =
            generateTestDataSet();
    private Config config;
    private CassandraContainer<?> container;
    private CqlSession session;

    @TestTemplate
    public void testCassandra(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob(CASSANDRA_JOB_CONFIG);
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertNotNull(getRow());
        compareResult();
        clearSinkTable();
        Assertions.assertNull(getRow());
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.container =
                new CassandraContainer<>(CASSANDRA_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(CASSANDRA_DOCKER_IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", PORT, PORT)));
        Startables.deepStart(Stream.of(this.container)).join();
        log.info("Cassandra container started");
        Awaitility.given()
                .ignoreExceptions()
                .await()
                .atMost(180L, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
        this.initializeCassandraTable();
        this.batchInsertData();
    }

    private void initializeCassandraTable() {
        initCassandraConfig();
        createKeyspace();
        try {
            session.execute(
                    SimpleStatement.builder(config.getString(SOURCE_TABLE))
                            .setKeyspace(KEYSPACE)
                            .setTimeout(Duration.ofSeconds(10))
                            .build());
            session.execute(
                    SimpleStatement.builder(config.getString(SINK_TABLE))
                            .setKeyspace(KEYSPACE)
                            .setTimeout(Duration.ofSeconds(10))
                            .build());
        } catch (Exception e) {
            throw new RuntimeException("Initializing Cassandra table failed!", e);
        }
    }

    private void initConnection() {
        try {
            File file = new File(CASSANDRA_DRIVER_CONFIG);
            this.session =
                    CqlSession.builder()
                            .addContactPoint(
                                    new InetSocketAddress(
                                            container.getHost(),
                                            container.getExposedPorts().get(0)))
                            .withLocalDatacenter(DATACENTER)
                            .withConfigLoader(DriverConfigLoader.fromFile(file))
                            .build();
        } catch (Exception e) {
            throw new RuntimeException("Init connection failed!", e);
        }
    }

    private void batchInsertData() {
        try {
            BatchStatement batchStatement = BatchStatement.builder(BatchType.UNLOGGED).build();
            BoundStatement boundStatement =
                    session.prepare(
                                    SimpleStatement.builder(config.getString(INSERT_CQL))
                                            .setKeyspace(KEYSPACE)
                                            .build())
                            .bind();
            for (SeaTunnelRow row : TEST_DATASET.getValue()) {
                boundStatement =
                        boundStatement
                                .setLong(0, (Long) row.getField(0))
                                .setString(1, (String) row.getField(1))
                                .setLong(2, (Long) row.getField(2))
                                .setByteBuffer(3, (ByteBuffer) row.getField(3))
                                .setBoolean(4, (Boolean) row.getField(4))
                                .setBigDecimal(5, (BigDecimal) row.getField(5))
                                .setDouble(6, (Double) row.getField(6))
                                .setFloat(7, (Float) row.getField(7))
                                .setInt(8, (Integer) row.getField(8))
                                .setInstant(9, (Instant) row.getField(9))
                                .setUuid(10, (UUID) row.getField(10))
                                .setString(11, (String) row.getField(11))
                                .setBigInteger(12, (BigInteger) row.getField(12))
                                .setUuid(13, (UUID) row.getField(13))
                                .setInetAddress(14, (InetAddress) row.getField(14))
                                .setLocalDate(15, (LocalDate) row.getField(15))
                                .setShort(16, (Short) row.getField(16))
                                .setByte(17, (Byte) row.getField(17))
                                .setList(18, (List<Float>) row.getField(18), Float.class)
                                .setList(19, (List<Integer>) row.getField(19), Integer.class)
                                .setSet(20, (Set<Double>) row.getField(20), Double.class)
                                .setSet(21, (Set<Long>) row.getField(21), Long.class)
                                .setMap(
                                        22,
                                        (Map<String, Integer>) row.getField(22),
                                        String.class,
                                        Integer.class);
                batchStatement = batchStatement.add(boundStatement);
            }
            session.execute(batchStatement);
            batchStatement.clear();
        } catch (Exception e) {
            throw new RuntimeException("Batch insert data failed!", e);
        }
    }

    private void compareResult() throws IOException {
        String sourceCql = "select * from " + SOURCE_TABLE;
        String sinkCql = "select * from " + SINK_TABLE;

        List<String> columnList =
                Arrays.stream(generateTestDataSet().getKey().getFieldNames())
                        .collect(Collectors.toList());
        ResultSet sourceResultSet =
                session.execute(SimpleStatement.builder(sourceCql).setKeyspace(KEYSPACE).build());
        ResultSet sinkResultSet =
                session.execute(SimpleStatement.builder(sinkCql).setKeyspace(KEYSPACE).build());
        Assertions.assertEquals(
                sourceResultSet.getColumnDefinitions().size(),
                sinkResultSet.getColumnDefinitions().size());
        Iterator<Row> sourceIterator = sourceResultSet.iterator();
        Iterator<Row> sinkIterator = sinkResultSet.iterator();
        while (sourceIterator.hasNext()) {
            if (sinkIterator.hasNext()) {
                Row sourceNext = sourceIterator.next();
                Row sinkNext = sinkIterator.next();
                for (String column : columnList) {
                    Object source = sourceNext.getObject(column);
                    Object sink = sinkNext.getObject(column);
                    if (!Objects.deepEquals(source, sink)) {
                        InputStream sourceAsciiStream =
                                sourceNext.get(column, ByteArrayInputStream.class);
                        InputStream sinkAsciiStream =
                                sinkNext.get(column, ByteArrayInputStream.class);
                        Assertions.assertNotNull(sourceAsciiStream);
                        Assertions.assertNotNull(sinkAsciiStream);
                        String sourceValue =
                                IOUtils.toString(sourceAsciiStream, StandardCharsets.UTF_8);
                        String sinkValue =
                                IOUtils.toString(sinkAsciiStream, StandardCharsets.UTF_8);
                        Assertions.assertEquals(sourceValue, sinkValue);
                    }
                    Assertions.assertTrue(true);
                }
            }
        }
    }

    private void createKeyspace() {
        try {
            this.session.execute(
                    "CREATE KEYSPACE IF NOT EXISTS "
                            + KEYSPACE
                            + " WITH replication = \n"
                            + "{'class':'SimpleStrategy','replication_factor':'1'};");
        } catch (Exception e) {
            throw new RuntimeException("Create keyspace failed!", e);
        }
    }

    private void clearSinkTable() {
        try {
            session.execute(
                    SimpleStatement.builder(String.format("truncate table %s", SINK_TABLE))
                            .setKeyspace(KEYSPACE)
                            .build());
        } catch (Exception e) {
            throw new RuntimeException("Test Cassandra server image failed!", e);
        }
    }

    private static Pair<SeaTunnelRowType, List<SeaTunnelRow>> generateTestDataSet() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {
                            "id",
                            "c_ascii",
                            "c_bigint",
                            "c_blob",
                            "c_boolean",
                            "c_decimal",
                            "c_double",
                            "c_float",
                            "c_int",
                            "c_timestamp",
                            "c_uuid",
                            "c_text",
                            "c_varint",
                            "c_timeuuid",
                            "c_inet",
                            "c_date",
                            "c_smallint",
                            "c_tinyint",
                            "c_list_float",
                            "c_list_int",
                            "c_set_double",
                            "c_set_bigint",
                            "c_map"
                        },
                        new SeaTunnelDataType[] {
                            BasicType.LONG_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.LONG_TYPE,
                            ArrayType.BYTE_ARRAY_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            new DecimalType(9, 4),
                            BasicType.DOUBLE_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.INT_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            LocalTimeType.LOCAL_DATE_TYPE,
                            BasicType.SHORT_TYPE,
                            BasicType.BYTE_TYPE,
                            ArrayType.FLOAT_ARRAY_TYPE,
                            ArrayType.INT_ARRAY_TYPE,
                            ArrayType.DOUBLE_ARRAY_TYPE,
                            ArrayType.LONG_ARRAY_TYPE,
                            new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE)
                        });
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 50; ++i) {
            SeaTunnelRow row;
            try {
                row =
                        new SeaTunnelRow(
                                new Object[] {
                                    (long) i,
                                    String.valueOf(i),
                                    (long) i,
                                    ByteBuffer.wrap(new byte[] {Byte.parseByte("1")}),
                                    Boolean.FALSE,
                                    BigDecimal.valueOf(11L, 2),
                                    Double.parseDouble("1.1"),
                                    Float.parseFloat("2.1"),
                                    i,
                                    Instant.now(),
                                    UUID.randomUUID(),
                                    "text",
                                    new BigInteger("12345678909876543210"),
                                    Uuids.timeBased(),
                                    InetAddress.getByName("1.2.3.4"),
                                    LocalDate.now(),
                                    Short.parseShort("1"),
                                    Byte.parseByte("1"),
                                    Collections.singletonList((float) i),
                                    Collections.singletonList(i),
                                    Collections.singleton(Double.valueOf("1.1")),
                                    Collections.singleton((long) i),
                                    Collections.singletonMap("key_" + i, i)
                                });
            } catch (UnknownHostException e) {
                throw new RuntimeException("Generate Test DataSet Failed!", e);
            }
            rows.add(row);
        }
        return Pair.of(rowType, rows);
    }

    private Row getRow() {
        try {
            String sql = String.format("select * from %s limit 1", SINK_TABLE);
            ResultSet resultSet =
                    session.execute(SimpleStatement.builder(sql).setKeyspace(KEYSPACE).build());
            return resultSet.one();
        } catch (Exception e) {
            throw new RuntimeException("test cassandra server image failed!", e);
        }
    }

    private void initCassandraConfig() {
        File file = ContainerUtil.getResourcesFile(INIT_CASSANDRA_PATH);
        Config config = ConfigFactory.parseFile(file);
        assert config.hasPath(SOURCE_TABLE)
                && config.hasPath(SINK_TABLE)
                && config.hasPath(INSERT_CQL);
        this.config = config;
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (this.session != null) {
            this.session.close();
        }
        if (this.container != null) {
            this.container.close();
        }
    }
}
