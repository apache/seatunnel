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

package jdbc.source;

import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.AbstractJdbcSourceChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class JdbcSourceChunkSplitterTest {

    @Test
    public void splitColumnTest() throws SQLException {
        TestJdbcSourceChunkSplitter testJdbcSourceChunkSplitter =
                new TestJdbcSourceChunkSplitter(null, new TestSourceDialect());
        Column splitColumn =
                testJdbcSourceChunkSplitter.getSplitColumn(
                        null, new TestSourceDialect(), new TableId("", "", ""));
        Assertions.assertEquals(splitColumn.typeName(), "tinyint");
    }

    private class TestJdbcSourceChunkSplitter extends AbstractJdbcSourceChunkSplitter {

        public TestJdbcSourceChunkSplitter(
                JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
            super(sourceConfig, dialect);
        }

        @Override
        public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
                throws SQLException {
            return new Object[0];
        }

        @Override
        public Object queryMin(
                JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
                throws SQLException {
            return null;
        }

        @Override
        public Object[] sampleDataFromColumn(
                JdbcConnection jdbc, TableId tableId, String columnName, int samplingRate)
                throws SQLException {
            return new Object[0];
        }

        @Override
        public Object queryNextChunkMax(
                JdbcConnection jdbc,
                TableId tableId,
                String columnName,
                int chunkSize,
                Object includedLowerBound)
                throws SQLException {
            return null;
        }

        @Override
        public Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
                throws SQLException {
            return null;
        }

        @Override
        public String buildSplitScanQuery(
                TableId tableId,
                SeaTunnelRowType splitKeyType,
                boolean isFirstSplit,
                boolean isLastSplit) {
            return null;
        }

        @Override
        public SeaTunnelDataType<?> fromDbzColumn(Column splitColumn) {
            String typeName = splitColumn.typeName();
            switch (typeName) {
                case "varchar":
                    return BasicType.STRING_TYPE;
                case "tinyint":
                    return BasicType.BYTE_TYPE;
                case "smallint":
                    return BasicType.SHORT_TYPE;
                case "int":
                    return BasicType.INT_TYPE;
                case "bigint":
                    return BasicType.LONG_TYPE;
                case "decimal":
                    return new DecimalType(20, 0);
                default:
                    return BasicType.STRING_TYPE;
            }
        }

        @Override
        public Column getSplitColumn(
                JdbcConnection jdbc, JdbcDataSourceDialect dialect, TableId tableId)
                throws SQLException {
            return super.getSplitColumn(jdbc, dialect, tableId);
        }
    }

    private class TestSourceDialect implements JdbcDataSourceDialect {

        @Override
        public String getName() {
            return null;
        }

        @Override
        public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
            return false;
        }

        @Override
        public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
            return null;
        }

        @Override
        public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
            return null;
        }

        @Override
        public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
            return null;
        }

        @Override
        public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {

            Table table =
                    Table.editor()
                            .tableId(tableId)
                            .addColumns(
                                    Column.editor()
                                            .name("string_col")
                                            .jdbcType(Types.VARCHAR)
                                            .type("varchar")
                                            .create(),
                                    Column.editor()
                                            .name("smallint")
                                            .jdbcType(Types.SMALLINT)
                                            .type("smallint")
                                            .create(),
                                    Column.editor()
                                            .name("int")
                                            .jdbcType(Types.INTEGER)
                                            .type("int")
                                            .create(),
                                    Column.editor()
                                            .name("decimal")
                                            .jdbcType(Types.DECIMAL)
                                            .type("decimal")
                                            .create(),
                                    Column.editor()
                                            .name("tinyint_col")
                                            .jdbcType(Types.TINYINT)
                                            .type("tinyint")
                                            .create(),
                                    Column.editor()
                                            .name("bigint_col")
                                            .jdbcType(Types.BIGINT)
                                            .type("bigint")
                                            .create())
                            .create();
            return new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table);
        }

        @Override
        public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
            return null;
        }

        @Override
        public JdbcSourceFetchTaskContext createFetchTaskContext(
                SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {
            return null;
        }

        @Override
        public Optional<PrimaryKey> getPrimaryKey(JdbcConnection jdbcConnection, TableId tableId)
                throws SQLException {
            return Optional.of(
                    PrimaryKey.of(
                            "pkName",
                            Arrays.asList(
                                    "string_col",
                                    "smallint",
                                    "int",
                                    "decimal",
                                    "tinyint_col",
                                    "bigint_col")));
        }

        @Override
        public List<ConstraintKey> getUniqueKeys(JdbcConnection jdbcConnection, TableId tableId)
                throws SQLException {
            return new ArrayList<ConstraintKey>();
        }
    }
}
