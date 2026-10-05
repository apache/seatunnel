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

package org.apache.seatunnel.edge.agent.starter.wal;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.edge.agent.starter.config.AgentRuntimeConfig;
import org.apache.seatunnel.edge.agent.starter.config.AgentSchedulerConfig;
import org.apache.seatunnel.edge.agent.starter.config.AgentSectionConfig;
import org.apache.seatunnel.edge.agent.starter.config.EdgeAgentRuntimeOptions;
import org.apache.seatunnel.edge.agent.starter.config.QueueConfig;
import org.apache.seatunnel.edge.agent.starter.config.RetryConfig;
import org.apache.seatunnel.edge.agent.starter.wal.mem.MemWalStore;
import org.apache.seatunnel.edge.agent.starter.wal.mem.MemWalStoreFactory;
import org.apache.seatunnel.edge.agent.starter.wal.sqlite.SqliteWalStore;
import org.apache.seatunnel.edge.agent.starter.wal.sqlite.SqliteWalStoreFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class WalStoreFactoryTest {

    @TempDir Path tempDir;

    @Test
    void memFactoryIdentifier() {
        MemWalStoreFactory factory = new MemWalStoreFactory();
        Assertions.assertEquals("mem", factory.factoryIdentifier());
    }

    @Test
    void sqliteFactoryIdentifier() {
        SqliteWalStoreFactory factory = new SqliteWalStoreFactory();
        Assertions.assertEquals("sqlite", factory.factoryIdentifier());
    }

    @Test
    void memFactoryOptionRuleIsEmpty() {
        MemWalStoreFactory factory = new MemWalStoreFactory();
        Assertions.assertNotNull(factory.optionRule());
        Assertions.assertTrue(factory.optionRule().getOptionalOptions().isEmpty());
        Assertions.assertTrue(factory.optionRule().getRequiredOptions().isEmpty());
    }

    @Test
    void sqliteFactoryOptionRuleIsNonEmpty() {
        SqliteWalStoreFactory factory = new SqliteWalStoreFactory();
        Assertions.assertNotNull(factory.optionRule());
    }

    @Test
    void memFactoryCreatesMemWalStore() throws Exception {
        MemWalStoreFactory factory = new MemWalStoreFactory();
        AgentRuntimeConfig config = buildMinimalConfig("data/wal.db");
        WalStore store = factory.create(config, tempDir);
        Assertions.assertInstanceOf(MemWalStore.class, store);
        store.close();
    }

    @Test
    void sqliteFactoryCreatesSqliteWalStoreWithRelativePath() throws Exception {
        SqliteWalStoreFactory factory = new SqliteWalStoreFactory();
        AgentRuntimeConfig config = buildMinimalConfig("test-wal.db");
        WalStore store = factory.create(config, tempDir);
        Assertions.assertInstanceOf(SqliteWalStore.class, store);
        Assertions.assertTrue(tempDir.resolve("test-wal.db").toFile().exists());
        store.close();
    }

    @Test
    void sqliteFactoryCreatesSqliteWalStoreWithAbsolutePath() throws Exception {
        SqliteWalStoreFactory factory = new SqliteWalStoreFactory();
        Path absolutePath = tempDir.resolve("abs-wal.db");
        AgentRuntimeConfig config = buildMinimalConfig(absolutePath.toString());
        WalStore store = factory.create(config, tempDir);
        Assertions.assertInstanceOf(SqliteWalStore.class, store);
        Assertions.assertTrue(absolutePath.toFile().exists());
        store.close();
    }

    @Test
    void discoverMemFactoryViaSpi() {
        WalStoreFactory factory =
                FactoryUtil.discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        WalStoreFactory.class,
                        "mem");
        Assertions.assertNotNull(factory);
        Assertions.assertInstanceOf(MemWalStoreFactory.class, factory);
    }

    @Test
    void discoverSqliteFactoryViaSpi() {
        WalStoreFactory factory =
                FactoryUtil.discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        WalStoreFactory.class,
                        "sqlite");
        Assertions.assertNotNull(factory);
        Assertions.assertInstanceOf(SqliteWalStoreFactory.class, factory);
    }

    @Test
    void discoverUnknownFactoryThrows() {
        Assertions.assertThrows(
                Exception.class,
                () ->
                        FactoryUtil.discoverFactory(
                                Thread.currentThread().getContextClassLoader(),
                                WalStoreFactory.class,
                                "unknown-store-type"));
    }

    @Test
    void sqliteFactoryNullWorkDirWithRelativePathThrows() {
        SqliteWalStoreFactory factory = new SqliteWalStoreFactory();
        AgentRuntimeConfig config = buildMinimalConfig("relative/wal.db");
        Assertions.assertThrows(NullPointerException.class, () -> factory.create(config, null));
    }

    private AgentRuntimeConfig buildMinimalConfig(String sqlitePath) {
        Map<String, Object> agentMap = new HashMap<>();
        agentMap.put(EdgeAgentRuntimeOptions.AGENT_ID.key(), "test-agent");
        agentMap.put(EdgeAgentRuntimeOptions.DELIVERY_GUARANTEE.key(), "BEST_EFFORT");

        Map<String, Object> queueMap = new HashMap<>();
        queueMap.put(EdgeAgentRuntimeOptions.QUEUE_SQLITE_PATH.key(), sqlitePath);

        Map<String, Object> schedulerMap = new HashMap<>();
        Map<String, Object> retryMap = new HashMap<>();

        AgentSectionConfig agent = AgentSectionConfig.from(ReadonlyConfig.fromMap(agentMap));
        QueueConfig queue = QueueConfig.from(ReadonlyConfig.fromMap(queueMap));
        AgentSchedulerConfig scheduler =
                AgentSchedulerConfig.from(ReadonlyConfig.fromMap(schedulerMap));
        RetryConfig retry = RetryConfig.from(ReadonlyConfig.fromMap(retryMap));

        return AgentRuntimeConfig.compose(agent, queue, scheduler, retry);
    }
}
