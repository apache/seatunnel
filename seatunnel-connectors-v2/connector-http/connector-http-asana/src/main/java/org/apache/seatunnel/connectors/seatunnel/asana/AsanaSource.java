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

package org.apache.seatunnel.connectors.seatunnel.asana;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.asana.config.AsanaSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpPaginationType;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.asana.config.AsanaSourceParameter;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class AsanaSource extends HttpSource {
    private final AsanaSourceParameter asanaSourceParameter = new AsanaSourceParameter();

    public AsanaSource(ReadonlyConfig pluginConfig) {
        super(pluginConfig);
        asanaSourceParameter.buildWithConfig(pluginConfig, pluginConfig.get(AsanaSourceOptions.API_KEY));

        TableSchema.Builder schema = TableSchema.builder();
        Map<String, String> fields = new LinkedHashMap<>();

        fields.put("gid", "$.data[*].gid");
        fields.put("name", "$.data[*].name");
        fields.put("completed", "$.data[*].completed");
        fields.put("completed_at", "$.data[*].completed_at");
        fields.put("created_at", "$.data[*].created_at");
        fields.put("modified_at", "$.data[*].modified_at");
        fields.put("due_on", "$.data[*].due_on");
        fields.put("assignee_gid", "$.data[*].assignee.gid");
        fields.put("assignee_name", "$.data[*].assignee.name");
        fields.put("permalink_url", "$.data[*].permalink_url");

        fields.keySet().forEach(c ->
                schema.column(PhysicalColumn.of(c, BasicType.STRING_TYPE, 0, true, null, null)));
        this.catalogTable = CatalogTable.of(TableIdentifier.of("Asana", TablePath.DEFAULT),
                schema.build(), Collections.emptyMap(), Collections.emptyList(), null);
        this.deserializationSchema = new JsonDeserializationSchema(catalogTable, false, false);
        this.jsonField = JsonField.builder().fields(fields).build();

        PageInfo info = new PageInfo();
        info.setPageType(HttpPaginationType.CURSOR.getCode());
        info.setPageCursorFieldName("offset");
        info.setPageCursorResponseField("$.next_page.offset");
        info.setPageIndex(HttpSourceOptions.START_PAGE_NUMBER.defaultValue());
        info.setBatchSize(HttpSourceOptions.BATCH_SIZE.defaultValue());
        info.setTotalPageSize(HttpSourceOptions.TOTAL_PAGE_SIZE.defaultValue());
        info.setUsePlaceholderReplacement(false);
        this.pageInfo = info;

    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new AsanaSourceReader(
                this.asanaSourceParameter, readerContext, this.deserializationSchema, jsonField, contentField, pageInfo);
    }

    @Override
    public String getPluginName() {
        return "Asana";
    }

    @Override public Boundedness getBoundedness() {
        if (JobMode.STREAMING.equals(jobContext.getJobMode())) {
            throw new HttpConnectorException(CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "Asana source only supports batch mode.");
        }
        return Boundedness.BOUNDED;
    }

}
