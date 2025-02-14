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

package org.apache.seatunnel.connectors.seatunnel.http.source;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class HttpSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    protected final HttpParameter httpParameter = new HttpParameter();
    protected PageInfo pageInfo;
    protected JsonField jsonField;
    protected String contentField;
    protected JobContext jobContext;
    protected DeserializationSchema<SeaTunnelRow> deserializationSchema;

    protected CatalogTable catalogTable;

    public HttpSource(Config pluginConfig) {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, HttpConfig.URL.key());
        if (!result.isSuccess()) {
            throw new HttpConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }

        this.httpParameter.buildWithConfig(pluginConfig);
        buildSchemaWithConfig(pluginConfig);
        buildPagingWithConfig(pluginConfig);
    }

    @Override
    public String getPluginName() {
        return HttpConfig.CONNECTOR_IDENTITY;
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    private void buildPagingWithConfig(Config pluginConfig) {
        if (pluginConfig.hasPath(HttpConfig.PAGEING.key())) {
            pageInfo = new PageInfo();
            Config pageConfig = pluginConfig.getConfig(HttpConfig.PAGEING.key());
            if (pageConfig.hasPath(HttpConfig.TOTAL_PAGE_SIZE.key())) {
                pageInfo.setTotalPageSize(pageConfig.getLong(HttpConfig.TOTAL_PAGE_SIZE.key()));
            } else {
                pageInfo.setTotalPageSize(HttpConfig.TOTAL_PAGE_SIZE.defaultValue());
            }
            if (pageConfig.hasPath(HttpConfig.START_PAGE_NUMBER.key())) {
                pageInfo.setPageIndex(pageConfig.getLong(HttpConfig.START_PAGE_NUMBER.key()));
            } else {
                pageInfo.setPageIndex(HttpConfig.START_PAGE_NUMBER.defaultValue());
            }

            if (pageConfig.hasPath(HttpConfig.BATCH_SIZE.key())) {
                pageInfo.setBatchSize(pageConfig.getInt(HttpConfig.BATCH_SIZE.key()));
            } else {
                pageInfo.setBatchSize(HttpConfig.BATCH_SIZE.defaultValue());
            }
            if (pageConfig.hasPath(HttpConfig.PAGE_FIELD.key())) {
                pageInfo.setPageField(pageConfig.getString(HttpConfig.PAGE_FIELD.key()));
            }
        }
    }

    protected void buildSchemaWithConfig(Config pluginConfig) {
        if (pluginConfig.hasPath(ConnectorCommonOptions.SCHEMA.key())) {
            this.catalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
            // default use json format
            HttpConfig.ResponseFormat format = HttpConfig.FORMAT.defaultValue();
            if (pluginConfig.hasPath(HttpConfig.FORMAT.key())) {
                format =
                        HttpConfig.ResponseFormat.valueOf(
                                pluginConfig
                                        .getString(HttpConfig.FORMAT.key())
                                        .toUpperCase(Locale.ROOT));
            }
            switch (format) {
                case JSON:
                    this.deserializationSchema =
                            new JsonDeserializationSchema(catalogTable, false, false);
                    if (pluginConfig.hasPath(HttpConfig.JSON_FIELD.key())) {
                        jsonField =
                                getJsonField(pluginConfig.getConfig(HttpConfig.JSON_FIELD.key()));
                    }
                    if (pluginConfig.hasPath(HttpConfig.CONTENT_FIELD.key())) {
                        contentField = pluginConfig.getString(HttpConfig.CONTENT_FIELD.key());
                    }
                    break;
                default:
                    // TODO: use format SPI
                    throw new HttpConnectorException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            String.format(
                                    "Unsupported data format [%s], http connector only support json format now",
                                    format));
            }
        } else {
            TableIdentifier tableIdentifier =
                    TableIdentifier.of(HttpConfig.CONNECTOR_IDENTITY, TablePath.DEFAULT);
            TableSchema tableSchema =
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "content", BasicType.STRING_TYPE, 0, false, null, null))
                            .build();

            this.catalogTable =
                    CatalogTable.of(
                            tableIdentifier,
                            tableSchema,
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            null);
            this.deserializationSchema =
                    new SimpleTextDeserializationSchema(catalogTable.getSeaTunnelRowType());
        }
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Lists.newArrayList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new HttpSourceReader(
                this.httpParameter,
                readerContext,
                this.deserializationSchema,
                jsonField,
                contentField,
                pageInfo);
    }

    private JsonField getJsonField(Config jsonFieldConf) {
        ConfigRenderOptions options = ConfigRenderOptions.concise();
        return JsonField.builder()
                .fields(JsonUtils.toMap(jsonFieldConf.root().render(options)))
                .build();
    }
}
