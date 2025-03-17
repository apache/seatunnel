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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog.ElasticSearchTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class ElasticsearchSource
        implements SeaTunnelSource<
                        SeaTunnelRow, ElasticsearchSourceSplit, ElasticsearchSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final List<ElasticsearchConfig> elasticsearchConfigList;
    private final ReadonlyConfig connectionConfig;

    public ElasticsearchSource(ReadonlyConfig config) {
        this.connectionConfig = config;
        boolean multiSource = config.getOptional(ElasticsearchSourceOptions.INDEX_LIST).isPresent();
        boolean singleSource = config.getOptional(ElasticsearchSourceOptions.INDEX).isPresent();
        if (multiSource && singleSource) {
            log.warn(
                    "Elasticsearch Source config warn: when both 'index' and 'index_list' are present in the configuration, only the 'index_list' configuration will take effect");
        }
        if (!multiSource && !singleSource) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.SOURCE_CONFIG_ERROR_01,
                    ElasticsearchConnectorErrorCode.SOURCE_CONFIG_ERROR_01.getDescription());
        }
        if (multiSource) {
            this.elasticsearchConfigList = createMultiSource(config);
        } else {
            this.elasticsearchConfigList =
                    Collections.singletonList(parseOneIndexQueryConfig(config));
        }
    }

    private List<ElasticsearchConfig> createMultiSource(ReadonlyConfig config) {
        List<Map<String, Object>> configMaps = config.get(ElasticsearchSourceOptions.INDEX_LIST);
        List<ReadonlyConfig> configList =
                configMaps.stream().map(ReadonlyConfig::fromMap).collect(Collectors.toList());
        List<ElasticsearchConfig> elasticsearchConfigList = new ArrayList<>(configList.size());
        for (ReadonlyConfig readonlyConfig : configList) {
            ElasticsearchConfig elasticsearchConfig = parseOneIndexQueryConfig(readonlyConfig);
            elasticsearchConfigList.add(elasticsearchConfig);
        }
        return elasticsearchConfigList;
    }

    private ElasticsearchConfig parseOneIndexQueryConfig(ReadonlyConfig readonlyConfig) {

        Map<String, Object> query = readonlyConfig.get(ElasticsearchSourceOptions.QUERY);
        String index = readonlyConfig.get(ElasticsearchSourceOptions.INDEX);

        CatalogTable catalogTable;
        List<String> source;
        Map<String, String> arrayColumn;

        if (readonlyConfig.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            // todo: We need to remove the schema in ES.
            log.warn(
                    "The schema config in ElasticSearch source/sink is deprecated, please use source config instead!");
            catalogTable = CatalogTableUtil.buildWithConfig(readonlyConfig);
            source = Arrays.asList(catalogTable.getSeaTunnelRowType().getFieldNames());
        } else {
            source = readonlyConfig.get(ElasticsearchSourceOptions.SOURCE);
            arrayColumn = readonlyConfig.get(ElasticsearchSourceOptions.ARRAY_COLUMN);
            Map<String, BasicTypeDefine<EsType>> esFieldType = getFieldTypeMapping(index, source);
            if (CollectionUtils.isEmpty(source)) {
                source = new ArrayList<>(esFieldType.keySet());
            }
            SeaTunnelDataType[] fieldTypes = getSeaTunnelDataType(esFieldType, source);
            TableSchema.Builder builder = TableSchema.builder();

            for (int i = 0; i < source.size(); i++) {
                String key = source.get(i);
                String sourceType = esFieldType.get(key).getDataType();
                if (arrayColumn.containsKey(key)) {
                    String value = arrayColumn.get(key);
                    SeaTunnelDataType<?> dataType =
                            SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(key, value);
                    builder.column(
                            PhysicalColumn.of(
                                    key, dataType, 0L, true, null, null, sourceType, null));
                    continue;
                }

                builder.column(
                        PhysicalColumn.of(
                                source.get(i),
                                fieldTypes[i],
                                0L,
                                true,
                                null,
                                null,
                                sourceType,
                                null));
            }
            catalogTable =
                    CatalogTable.of(
                            TableIdentifier.of("elasticsearch", null, index),
                            builder.build(),
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            "");
        }

        String scrollTime = readonlyConfig.get(ElasticsearchSourceOptions.SCROLL_TIME);
        int scrollSize = readonlyConfig.get(ElasticsearchSourceOptions.SCROLL_SIZE);
        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig();
        elasticsearchConfig.setSource(source);
        elasticsearchConfig.setCatalogTable(catalogTable);
        elasticsearchConfig.setQuery(query);
        elasticsearchConfig.setScrollTime(scrollTime);
        elasticsearchConfig.setScrollSize(scrollSize);
        elasticsearchConfig.setIndex(index);
        elasticsearchConfig.setCatalogTable(catalogTable);
        return elasticsearchConfig;
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
        return elasticsearchConfigList.stream()
                .map(ElasticsearchConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, ElasticsearchSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new ElasticsearchSourceReader(readerContext, connectionConfig);
    }

    @Override
    public SourceSplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceState>
            createEnumerator(
                    SourceSplitEnumerator.Context<ElasticsearchSourceSplit> enumeratorContext) {
        return new ElasticsearchSourceSplitEnumerator(
                enumeratorContext, connectionConfig, elasticsearchConfigList);
    }

    @Override
    public SourceSplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<ElasticsearchSourceSplit> enumeratorContext,
                    ElasticsearchSourceState sourceState) {
        return new ElasticsearchSourceSplitEnumerator(
                enumeratorContext, sourceState, connectionConfig, elasticsearchConfigList);
    }

    @VisibleForTesting
    public static SeaTunnelDataType[] getSeaTunnelDataType(
            Map<String, BasicTypeDefine<EsType>> esFieldType, List<String> source) {
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[source.size()];
        for (int i = 0; i < source.size(); i++) {
            BasicTypeDefine<EsType> esType = esFieldType.get(source.get(i));
            SeaTunnelDataType<?> seaTunnelDataType =
                    ElasticSearchTypeConverter.INSTANCE.convert(esType).getDataType();
            fieldTypes[i] = seaTunnelDataType;
        }
        return fieldTypes;
    }

    private Map<String, BasicTypeDefine<EsType>> getFieldTypeMapping(
            String index, List<String> source) {
        // EsRestClient#getFieldTypeMapping may throw runtime exception
        // so here we use try-resources-finally to close the resource
        try (EsRestClient esRestClient = EsRestClient.createInstance(connectionConfig)) {
            return esRestClient.getFieldTypeMapping(index, source);
        }
    }
}
