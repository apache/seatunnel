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

package org.apache.seatunnel.connectors.seatunnel.openmldb.source;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbParameters;
import org.apache.seatunnel.connectors.seatunnel.openmldb.exception.OpenMldbConnectorException;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import com._4paradigm.openmldb.sdk.Column;
import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OpenMldbSourceTest {
    @Test
    void discoveryPreservesNullabilityAndClosesExecutor() throws Exception {
        Schema schema =
                new Schema(
                        Arrays.asList(
                                new Column("id", Types.VARCHAR, true, false),
                                new Column("value", Types.INTEGER, false, false)));
        try (MockedConstruction<SqlClusterExecutor> executors =
                mockConstruction(
                        SqlClusterExecutor.class,
                        (executor, context) ->
                                when(executor.getInputSchema(anyString(), anyString()))
                                        .thenReturn(schema))) {
            OpenMldbSource source = new OpenMldbSource(parameters());
            assertEquals(1, source.getProducedCatalogTables().size());
            assertFalse(
                    source.getProducedCatalogTables()
                            .get(0)
                            .getTableSchema()
                            .getColumns()
                            .get(0)
                            .isNullable());
            assertTrue(
                    source.getProducedCatalogTables()
                            .get(0)
                            .getTableSchema()
                            .getColumns()
                            .get(1)
                            .isNullable());
            verify(executors.constructed().get(0)).close();
        }
    }

    @Test
    void failedDiscoveryClosesExecutorAndPreservesCause() throws Exception {
        SQLException cause = new SQLException("invalid query");
        try (MockedConstruction<SqlClusterExecutor> executors =
                mockConstruction(
                        SqlClusterExecutor.class,
                        (executor, context) ->
                                when(executor.getInputSchema(anyString(), anyString()))
                                        .thenThrow(cause))) {
            OpenMldbConnectorException error =
                    assertThrows(
                            OpenMldbConnectorException.class,
                            () -> new OpenMldbSource(parameters()));
            assertEquals(cause, error.getCause());
            verify(executors.constructed().get(0)).close();
        }
    }

    @Test
    void multiTableMetadataDoesNotConnectAndSourceIsSerializable() throws Exception {
        ReadonlyConfig config =
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString(
                                "cluster_mode=false\nhost=unused\nport=6527\ndatabase=test\n"
                                        + "tables_configs=["
                                        + "{sql=\"select id from orders\",schema{table=orders,fields{id=STRING}}},"
                                        + "{sql=\"select amount from sales\",database=other,schema{table=sales,fields{amount=INT}}}"
                                        + "]"));
        try (MockedConstruction<SqlClusterExecutor> executors =
                mockConstruction(SqlClusterExecutor.class)) {
            OpenMldbSource source =
                    (OpenMldbSource)
                            new OpenMldbSourceFactory()
                                    .<SeaTunnelRow, SingleSplit, SingleSplitEnumeratorState>
                                            createSource(
                                                    new TableSourceFactoryContext(
                                                            config, getClass().getClassLoader()))
                                    .createSource();
            assertEquals(2, source.getProducedCatalogTables().size());
            assertEquals(
                    "orders",
                    source.getProducedCatalogTables().get(0).getTableId().toTablePath().toString());
            assertEquals(
                    "sales",
                    source.getProducedCatalogTables().get(1).getTableId().toTablePath().toString());
            assertTrue(executors.constructed().isEmpty());
            try (ObjectOutputStream out = new ObjectOutputStream(new ByteArrayOutputStream())) {
                out.writeObject(source);
            }
        }
    }

    private OpenMldbParameters parameters() {
        return OpenMldbParameters.buildWithConfig(
                ConfigFactory.parseString(
                        "cluster_mode=false\nhost=localhost\nport=6527\ndatabase=test\nsql=\"select * from values_table\""));
    }
}
