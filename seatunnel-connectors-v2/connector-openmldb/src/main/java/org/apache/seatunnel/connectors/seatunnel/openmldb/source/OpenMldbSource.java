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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbParameters;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbSqlExecutor;
import org.apache.seatunnel.connectors.seatunnel.openmldb.exception.OpenMldbConnectorException;

import com._4paradigm.openmldb.sdk.Column;
import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;

public class OpenMldbSource extends AbstractSingleSplitSource<SeaTunnelRow>
        implements SupportColumnProjection {
    private final OpenMldbParameters openMldbParameters;
    private final CatalogTable catalogTable;
    private JobContext jobContext;

    public OpenMldbSource(OpenMldbParameters openMldbParameters) {
        this.openMldbParameters = openMldbParameters;
        OpenMldbSqlExecutor.initSdkOption(openMldbParameters);
        try {
            SqlClusterExecutor sqlExecutor = OpenMldbSqlExecutor.getSqlExecutor();
            Schema inputSchema =
                    sqlExecutor.getInputSchema(
                            openMldbParameters.getDatabase(), openMldbParameters.getSql());
            List<Column> columnList = inputSchema.getColumnList();
            this.catalogTable = convert(columnList);
        } catch (SQLException | SqlException e) {
            throw new OpenMldbConnectorException(
                    CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED,
                    "Failed to initialize data schema");
        }
    }

    @Override
    public String getPluginName() {
        return "OpenMldb";
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new OpenMldbSourceReader(
                openMldbParameters, catalogTable.getSeaTunnelRowType(), readerContext);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    private SeaTunnelDataType<?> convertSeaTunnelDataType(int type) {
        switch (type) {
            case Types.BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case Types.INTEGER:
                return BasicType.INT_TYPE;
            case Types.SMALLINT:
                return BasicType.SHORT_TYPE;
            case Types.BIGINT:
                return BasicType.LONG_TYPE;
            case Types.FLOAT:
                return BasicType.FLOAT_TYPE;
            case Types.DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case Types.VARCHAR:
                return BasicType.STRING_TYPE;
            case Types.DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case Types.TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                throw new OpenMldbConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "SeaTunnel does not support this data type");
        }
    }

    private CatalogTable convert(List<Column> columnList) {
        TableSchema.Builder builder = TableSchema.builder();
        for (int i = 0; i < columnList.size(); i++) {
            Column column = columnList.get(i);
            builder.column(
                    PhysicalColumn.of(
                            column.getColumnName(),
                            convertSeaTunnelDataType(column.getSqlType()),
                            (Long) null,
                            column.isNotNull(),
                            null,
                            null));
        }
        return CatalogTable.of(
                TableIdentifier.of("OpenMldb", openMldbParameters.getDatabase(), "default"),
                builder.build(),
                null,
                null,
                null);
    }
}
