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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportDataSaveMode;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;

@AutoService(SeaTunnelSink.class)
public class JdbcSink
        implements SeaTunnelSink<SeaTunnelRow, JdbcSinkState, XidInfo, JdbcAggregatedCommitInfo>,
                SupportDataSaveMode,
                SupportMultiTableSink {

    private SeaTunnelRowType seaTunnelRowType;

    private JobContext jobContext;

    private JdbcSinkConfig jdbcSinkConfig;

    private JdbcDialect dialect;

    private ReadonlyConfig config;

    private DataSaveMode dataSaveMode;

    private CatalogTable catalogTable;

    public JdbcSink(
            ReadonlyConfig config,
            JdbcSinkConfig jdbcSinkConfig,
            JdbcDialect dialect,
            DataSaveMode dataSaveMode,
            CatalogTable catalogTable) {
        this.config = config;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.dialect = dialect;
        this.dataSaveMode = dataSaveMode;
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
    }

    public JdbcSink() {}

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.config = ReadonlyConfig.fromConfig(pluginConfig);
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.dialect =
                JdbcDialectLoader.load(
                        jdbcSinkConfig.getJdbcConnectionConfig().getUrl(),
                        jdbcSinkConfig.getJdbcConnectionConfig().getCompatibleMode(),
                        config.get(JdbcOptions.FIELD_IDE) == null
                                ? null
                                : config.get(JdbcOptions.FIELD_IDE).getValue());
        this.dialect.connectionUrlParse(
                jdbcSinkConfig.getJdbcConnectionConfig().getUrl(),
                jdbcSinkConfig.getJdbcConnectionConfig().getProperties(),
                this.dialect.defaultParameter());
        this.dataSaveMode = DataSaveMode.KEEP_SCHEMA_AND_DATA;
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> createWriter(
            SinkWriter.Context context) {
        SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> sinkWriter;
        if (jdbcSinkConfig.isExactlyOnce()) {
            sinkWriter =
                    new JdbcExactlyOnceSinkWriter(
                            context,
                            jobContext,
                            dialect,
                            jdbcSinkConfig,
                            seaTunnelRowType,
                            new ArrayList<>());
        } else {
            if (catalogTable != null && catalogTable.getTableSchema().getPrimaryKey() != null) {
                String keyName =
                        catalogTable.getTableSchema().getPrimaryKey().getColumnNames().get(0);
                int index = seaTunnelRowType.indexOf(keyName);
                if (index > -1) {
                    return new JdbcSinkWriter(
                            context, dialect, jdbcSinkConfig, seaTunnelRowType, index);
                }
            }
            sinkWriter =
                    new JdbcSinkWriter(context, dialect, jdbcSinkConfig, seaTunnelRowType, null);
        }
        return sinkWriter;
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> restoreWriter(
            SinkWriter.Context context, List<JdbcSinkState> states) throws IOException {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return new JdbcExactlyOnceSinkWriter(
                    context, jobContext, dialect, jdbcSinkConfig, seaTunnelRowType, states);
        }
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo>>
            createAggregatedCommitter() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new JdbcSinkAggregatedCommitter(jdbcSinkConfig));
        }
        return Optional.empty();
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public Optional<Serializer<JdbcAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Optional<Serializer<XidInfo>> getCommitInfoSerializer() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();
    }

    @Override
    public DataSaveMode getUserConfigSaveMode() {
        return dataSaveMode;
    }

    @Override
    public void handleSaveMode(DataSaveMode saveMode) {
        if (catalogTable != null) {
            if (StringUtils.isBlank(jdbcSinkConfig.getDatabase())) {
                return;
            }
            Optional<Catalog> catalogOptional =
                    JdbcCatalogUtils.findCatalog(jdbcSinkConfig.getJdbcConnectionConfig(), dialect);
            if (catalogOptional.isPresent()) {
                try (Catalog catalog = catalogOptional.get()) {
                    catalog.open();
                    FieldIdeEnum fieldIdeEnumEnum = config.get(JdbcOptions.FIELD_IDE);
                    String fieldIde =
                            fieldIdeEnumEnum == null
                                    ? FieldIdeEnum.ORIGINAL.getValue()
                                    : fieldIdeEnumEnum.getValue();
                    TablePath tablePath =
                            TablePath.of(
                                    jdbcSinkConfig.getDatabase()
                                            + "."
                                            + CatalogUtils.quoteTableIdentifier(
                                                    jdbcSinkConfig.getTable(), fieldIde));
                    if (!catalog.databaseExists(jdbcSinkConfig.getDatabase())) {
                        catalog.createDatabase(tablePath, true);
                    }
                    catalogTable.getOptions().put("fieldIde", fieldIde);
                    if (!catalog.tableExists(tablePath)) {
                        catalog.createTable(tablePath, catalogTable, true);
                    }
                } catch (UnsupportedOperationException | CatalogException e) {
                    // TODO Temporary fix, this feature has been changed in this pr
                    // https://github.com/apache/seatunnel/pull/5645
                } catch (Exception e) {
                    throw new JdbcConnectorException(HANDLE_SAVE_MODE_FAILED, e);
                }
            }
        }
    }
}
