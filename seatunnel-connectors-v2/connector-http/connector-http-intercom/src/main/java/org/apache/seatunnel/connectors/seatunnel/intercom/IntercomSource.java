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

package org.apache.seatunnel.connectors.seatunnel.intercom;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpPaginationType;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.intercom.config.IntercomSourceParameter;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Intercom contacts source. Reuses {@link HttpSource}, but after super(pluginConfig) replaces the
 * schema, JSONPath fields and paging so the eight fixed contact columns and the cursor pagination
 * ("pages.next.starting_after") are used instead of user-supplied config.
 */
@Slf4j
public class IntercomSource extends HttpSource {
    private final IntercomSourceParameter intercomSourceParameter = new IntercomSourceParameter();

    public IntercomSource(ReadonlyConfig pluginConfig) {
        super(pluginConfig);
        intercomSourceParameter.buildWithConfig(pluginConfig);

        TableSchema.Builder schema = TableSchema.builder();
        Map<String, String> fields = new LinkedHashMap<>();

        Map<String, SeaTunnelDataType<?>> types = new LinkedHashMap<>();
        types.put("id", BasicType.STRING_TYPE);
        types.put("role", BasicType.STRING_TYPE);
        types.put("email", BasicType.STRING_TYPE);
        types.put("name", BasicType.STRING_TYPE);
        types.put("phone", BasicType.STRING_TYPE);
        types.put("external_id", BasicType.STRING_TYPE);
        types.put("created_at", BasicType.LONG_TYPE);
        types.put("updated_at", BasicType.LONG_TYPE);
        types.keySet().forEach(c -> fields.put(c, "$.data[*]." + c));

        types.forEach(
                (name, type) -> schema.column(PhysicalColumn.of(name, type, 0, true, null, null)));
        this.catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("Intercom", TablePath.DEFAULT),
                        schema.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);
        this.deserializationSchema = new JsonDeserializationSchema(catalogTable, false, false);
        this.jsonField = JsonField.builder().fields(fields).build();

        PageInfo info = new PageInfo();
        info.setPageType(HttpPaginationType.CURSOR.getCode());
        info.setPageCursorFieldName("starting_after");
        info.setPageCursorResponseField("$.pages.next.starting_after");
        info.setPageIndex(HttpSourceOptions.START_PAGE_NUMBER.defaultValue());
        info.setBatchSize(HttpSourceOptions.BATCH_SIZE.defaultValue());
        info.setTotalPageSize(HttpSourceOptions.TOTAL_PAGE_SIZE.defaultValue());
        info.setUsePlaceholderReplacement(false);
        this.pageInfo = info;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new IntercomSourceReader(
                this.intercomSourceParameter,
                readerContext,
                this.deserializationSchema,
                jsonField,
                contentField,
                pageInfo);
    }

    @Override
    public String getPluginName() {
        return "Intercom";
    }

    @Override
    public Boundedness getBoundedness() {
        if (JobMode.STREAMING.equals(jobContext.getJobMode())) {
            throw new HttpConnectorException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "Intercom source only supports batch mode.");
        }
        return Boundedness.BOUNDED;
    }
}
