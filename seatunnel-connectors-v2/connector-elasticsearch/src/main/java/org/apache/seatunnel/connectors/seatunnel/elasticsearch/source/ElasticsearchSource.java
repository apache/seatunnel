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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog.ElasticSearchTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SourceConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ElasticsearchSource
        implements SeaTunnelSource<
                        SeaTunnelRow, ElasticsearchSourceSplit, ElasticsearchSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final ReadonlyConfig config;

    private CatalogTable catalogTable;

    private List<String> source;

    public ElasticsearchSource(ReadonlyConfig config) {
        this.config = config;
        if (config.getOptional(TableSchemaOptions.SCHEMA).isPresent()) {
            // todo: We need to remove the schema in ES.
            catalogTable = CatalogTableUtil.buildWithConfig(config);
            source = Arrays.asList(catalogTable.getSeaTunnelRowType().getFieldNames());
        } else {
            source = config.get(SourceConfig.SOURCE);
            EsRestClient esRestClient = EsRestClient.createInstance(config);
            Map<String, BasicTypeDefine<EsType>> esFieldType =
                    esRestClient.getFieldTypeMapping(config.get(SourceConfig.INDEX), source);
            esRestClient.close();
            SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[source.size()];
            for (int i = 0; i < source.size(); i++) {
                BasicTypeDefine<EsType> esType = esFieldType.get(source.get(i));
                SeaTunnelDataType<?> seaTunnelDataType =
                        ElasticSearchTypeConverter.INSTANCE.convert(esType).getDataType();
                fieldTypes[i] = seaTunnelDataType;
            }
            TableSchema.Builder builder = TableSchema.builder();
            for (int i = 0; i < source.size(); i++) {
                builder.column(
                        PhysicalColumn.of(source.get(i), fieldTypes[i], 0, true, null, null));
            }
            catalogTable =
                    CatalogTable.of(
                            TableIdentifier.of(
                                    "elasticsearch", null, config.get(SourceConfig.INDEX)),
                            builder.build(),
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            "");
        }
    }

    @Override
    public String getPluginName() {
        return "Elasticsearch";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, ElasticsearchSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new ElasticsearchSourceReader(
                readerContext, config, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public SourceSplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceState>
            createEnumerator(
                    SourceSplitEnumerator.Context<ElasticsearchSourceSplit> enumeratorContext) {
        return new ElasticsearchSourceSplitEnumerator(enumeratorContext, config, source);
    }

    @Override
    public SourceSplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<ElasticsearchSourceSplit> enumeratorContext,
                    ElasticsearchSourceState sourceState) {
        return new ElasticsearchSourceSplitEnumerator(
                enumeratorContext, sourceState, config, source);
    }
}
