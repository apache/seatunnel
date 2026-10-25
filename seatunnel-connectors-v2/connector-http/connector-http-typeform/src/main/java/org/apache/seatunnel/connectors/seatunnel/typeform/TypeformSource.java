/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.typeform;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
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
import org.apache.seatunnel.connectors.seatunnel.typeform.config.TypeformSourceParameter;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Batch source that reads one form's responses from the Typeform Responses API. Pages are read
 * newest first by passing the token of the last response on a page as the {@code before} cursor.
 * {@code answers} and {@code hidden} are emitted as raw JSON strings.
 */
@Slf4j
public class TypeformSource extends HttpSource {
    static final String[] COLUMNS = {
        "response_id", "landing_id", "token", "landed_at", "submitted_at", "answers", "hidden"
    };

    private final TypeformSourceParameter typeformSourceParameter = new TypeformSourceParameter();

    public TypeformSource(ReadonlyConfig pluginConfig) {
        super(pluginConfig);
        typeformSourceParameter.buildWithConfig(pluginConfig);

        TableSchema.Builder schema = TableSchema.builder();
        Map<String, String> fields = new LinkedHashMap<>();
        for (String column : COLUMNS) {
            fields.put(column, "$.items[*]." + column);
            schema.column(PhysicalColumn.of(column, BasicType.STRING_TYPE, 0, true, null, null));
        }
        this.catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("Typeform", TablePath.DEFAULT),
                        schema.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);
        this.deserializationSchema = new JsonDeserializationSchema(catalogTable, false, false);
        this.jsonField = JsonField.builder().fields(fields).build();

        PageInfo info = new PageInfo();
        info.setPageType(HttpPaginationType.CURSOR.getCode());
        info.setPageCursorFieldName("before");
        // The token of the last response on the page; an empty page yields no value and ends the
        // read.
        info.setPageCursorResponseField("$.items[-1:].token");
        info.setPageIndex(HttpSourceOptions.START_PAGE_NUMBER.defaultValue());
        info.setBatchSize(HttpSourceOptions.BATCH_SIZE.defaultValue());
        info.setTotalPageSize(HttpSourceOptions.TOTAL_PAGE_SIZE.defaultValue());
        info.setUsePlaceholderReplacement(false);
        this.pageInfo = info;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new TypeformSourceReader(
                this.typeformSourceParameter,
                readerContext,
                this.deserializationSchema,
                jsonField,
                contentField,
                pageInfo);
    }

    @Override
    public String getPluginName() {
        return "Typeform";
    }

    @Override
    public Boundedness getBoundedness() {
        if (JobMode.STREAMING.equals(jobContext.getJobMode())) {
            throw new HttpConnectorException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "Typeform source only supports batch mode.");
        }
        return Boundedness.BOUNDED;
    }
}
