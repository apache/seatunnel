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

package org.apache.seatunnel.connectors.seatunnel.fluss.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSinkOptions;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.TableWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class FlussSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private Connection connection;
    private TableWriter writer;
    private Table table;
    private String dbName;
    private String tableName;
    private final SeaTunnelRowType seaTunnelRowType;

    public FlussSinkWriter(
            SinkWriter.Context context, CatalogTable catalogTable, ReadonlyConfig pluginConfig) {
        seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        Configuration flussConfig = new Configuration();
        flussConfig.setString(
                FlussSinkOptions.BOOTSTRAP_SERVERS.key(),
                pluginConfig.get(FlussSinkOptions.BOOTSTRAP_SERVERS));
        Optional<Map<String, String>> clientConfig =
                pluginConfig.getOptional(FlussSinkOptions.CLIENT_CONFIG);
        if (clientConfig.isPresent()) {
            clientConfig
                    .get()
                    .forEach(
                            (k, v) -> {
                                flussConfig.setString(k, v);
                            });
        }
        log.info("Connect to Fluss with config: {}", flussConfig);
        connection = ConnectionFactory.createConnection(flussConfig);
        log.info("Connect to Fluss success");
        dbName =
                pluginConfig
                        .getOptional(FlussSinkOptions.DATABASE)
                        .orElseGet(() -> catalogTable.getTableId().getDatabaseName());
        tableName =
                pluginConfig
                        .getOptional(FlussSinkOptions.TABLE)
                        .orElseGet(() -> catalogTable.getTableId().getTableName());
        TablePath tablePath = TablePath.of(dbName, tableName);
        table = connection.getTable(tablePath);
        if (table.getTableInfo().hasPrimaryKey()) {
            log.info("Table {} has primary key, use upsert writer", tableName);
            writer = table.newUpsert().createWriter();
        } else {
            log.info("Table {} has no primary key, use append writer", tableName);
            writer = table.newAppend().createWriter();
        }
    }

    @Override
    public void write(SeaTunnelRow element) {
        RowKind rowKind = element.getRowKind();
        GenericRow genericRow = new GenericRow(element.getFields().length);
        for (int i = 0; i < element.getFields().length; i++) {
            genericRow.setField(
                    i,
                    convert(
                            seaTunnelRowType.getFieldType(i),
                            seaTunnelRowType.getFieldName(i),
                            element.getField(i)));
        }

        if (writer instanceof UpsertWriter) {
            UpsertWriter upsertWriter = (UpsertWriter) writer;
            switch (rowKind) {
                case INSERT:
                case UPDATE_AFTER:
                    upsertWriter.upsert(genericRow);
                    break;
                case DELETE:
                    upsertWriter.delete(genericRow);
                    break;
                case UPDATE_BEFORE:
                    return;
                default:
                    throw CommonError.unsupportedRowKind(
                            FlussSinkOptions.CONNECTOR_IDENTITY, tableName, rowKind.shortString());
            }
        } else if (writer instanceof AppendWriter) {
            AppendWriter appendWriter = (AppendWriter) writer;
            switch (rowKind) {
                case INSERT:
                case UPDATE_AFTER:
                    appendWriter.append(genericRow);
                    break;
                case DELETE:
                case UPDATE_BEFORE:
                    return;
                default:
                    throw CommonError.unsupportedRowKind(
                            FlussSinkOptions.CONNECTOR_IDENTITY, tableName, rowKind.shortString());
            }
        } else {
            throw CommonError.unsupportedOperation(
                    FlussSinkOptions.CONNECTOR_IDENTITY, writer.getClass().getName());
        }
    }

    @Override
    public Optional<Void> prepareCommit(long checkpointId) throws IOException {
        writer.flush();
        return super.prepareCommit(checkpointId);
    }

    @Override
    public void close() {
        log.info("Close Fluss table.");
        try {
            if (table != null) {
                table.close();
            }
        } catch (Exception e) {
            throw CommonError.closeFailed("Close Fluss table failed.", e);
        }

        log.info("Close Fluss connection.");
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            throw CommonError.closeFailed("Close Fluss connection failed.", e);
        }
    }

    protected Object convert(SeaTunnelDataType dataType, String fieldName, Object val) {
        if (val == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BYTES:
                return val;
            case STRING:
                return BinaryString.fromString((String) val);
            case DECIMAL:
                return Decimal.fromBigDecimal(
                        (BigDecimal) val,
                        ((DecimalType) dataType).getPrecision(),
                        ((DecimalType) dataType).getScale());
            case DATE:
                return (int) ((LocalDate) val).toEpochDay();
            case TIME:
                return (int) (((LocalTime) val).toNanoOfDay() / 1_000_000);
            case TIMESTAMP:
                return TimestampNtz.fromLocalDateTime((LocalDateTime) val);
            case TIMESTAMP_TZ:
                if (val instanceof Instant) {
                    return TimestampLtz.fromInstant((Instant) val);
                } else if (val instanceof OffsetDateTime) {
                    return TimestampLtz.fromInstant(((OffsetDateTime) val).toInstant());
                }
                throw CommonError.unsupportedDataType(
                        FlussSinkOptions.CONNECTOR_IDENTITY,
                        dataType.getSqlType().name(),
                        fieldName);
            default:
                throw CommonError.unsupportedDataType(
                        FlussSinkOptions.CONNECTOR_IDENTITY,
                        dataType.getSqlType().name(),
                        fieldName);
        }
    }
}
