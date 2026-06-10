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

package org.apache.seatunnel.connectors.seatunnel.cdc.opengauss;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Tests the OpenGauss CDC source entry point introduced by the PG-base refactor so the dedicated
 * factory/source wiring stays compatible.
 */
public class OpengaussIncrementalSourceFactoryTest {

    @Test
    public void testGetSourceClassUsesDedicatedOpengaussSource() {
        OpengaussIncrementalSourceFactory factory = new OpengaussIncrementalSourceFactory();

        Assertions.assertEquals(OpengaussIncrementalSource.class, factory.getSourceClass());
    }

    @Test
    public void testSourceKeepsOpengaussPluginName() {
        OpengaussIncrementalSource<Object> source =
                new TestingOpengaussIncrementalSource(
                        ReadonlyConfig.fromMap(Collections.emptyMap()), createCatalogTables());

        Assertions.assertEquals("Opengauss-CDC", source.getPluginName());
    }

    /** Builds a minimal catalog table list so the source constructor can initialize metadata. */
    private List<CatalogTable> createCatalogTables() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        return Collections.singletonList(
                CatalogTableUtil.getCatalogTable(
                        "opengauss", "test_db", "public", "orders", rowType));
    }

    /**
     * Lightweight test source that bypasses the real JDBC bootstrap so the regression test can
     * focus on the entry-path contract only.
     */
    private static final class TestingOpengaussIncrementalSource
            extends OpengaussIncrementalSource<Object> {

        private TestingOpengaussIncrementalSource(
                ReadonlyConfig options, List<CatalogTable> catalogTables) {
            super(options, catalogTables);
        }

        /**
         * Returns a mock config factory because this regression test does not need real JDBC setup.
         */
        @Override
        public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(
                ReadonlyConfig config) {
            return subtask -> mock(JdbcSourceConfig.class);
        }

        /**
         * Returns a mock dialect because the constructor should not hit an external database here.
         */
        @Override
        public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
            return mockDataSourceDialect();
        }

        /** Returns a mock deserializer because schema loading is outside this regression scope. */
        @Override
        public DebeziumDeserializationSchema<Object> createDebeziumDeserializationSchema(
                ReadonlyConfig config) {
            return mockDebeziumDeserializationSchema();
        }

        /** Returns a no-op offset factory so the constructor can finish without connector state. */
        @Override
        public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
            return new TestingOffsetFactory();
        }

        /** Creates a typed dialect mock for the minimal constructor-only regression scenario. */
        @SuppressWarnings("unchecked")
        private static DataSourceDialect<JdbcSourceConfig> mockDataSourceDialect() {
            return (DataSourceDialect<JdbcSourceConfig>) mock(DataSourceDialect.class);
        }

        /**
         * Creates a typed deserializer mock for the minimal constructor-only regression scenario.
         */
        @SuppressWarnings("unchecked")
        private static DebeziumDeserializationSchema<Object> mockDebeziumDeserializationSchema() {
            return (DebeziumDeserializationSchema<Object>)
                    mock(DebeziumDeserializationSchema.class);
        }
    }

    /** Minimal offset factory used to satisfy the source constructor in this regression test. */
    private static final class TestingOffsetFactory extends OffsetFactory {

        @Override
        public Offset earliest() {
            return null;
        }

        @Override
        public Offset neverStop() {
            return null;
        }

        @Override
        public Offset latest() {
            return null;
        }

        @Override
        public Offset specific(Map<String, String> offset) {
            return null;
        }

        @Override
        public Offset specific(String filename, Long position) {
            return null;
        }

        @Override
        public Offset timestamp(long timestamp) {
            return null;
        }
    }
}
