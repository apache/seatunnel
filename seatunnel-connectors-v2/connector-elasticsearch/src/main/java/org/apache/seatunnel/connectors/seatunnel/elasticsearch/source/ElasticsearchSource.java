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

import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog.ElasticSearchTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.PkConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SourceConfig;
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

    private final List<SourceConfig> sourceConfigList;
    private final ReadonlyConfig connectionConfig;

    private final int STR_PK_DEFAULT_LENGTH = 512 * 4;
    private final int STR_FIELD_DEFAULT_LENGTH = 2048 * 4;

    public ElasticsearchSource(ReadonlyConfig config) {
        this.connectionConfig = config;
        boolean multiSource = config.getOptional(SourceConfig.INDEX_LIST).isPresent();
        boolean singleSource = config.getOptional(SourceConfig.INDEX).isPresent();
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
            this.sourceConfigList = createMultiSource(config);
        } else {
            this.sourceConfigList = Collections.singletonList(parseOneIndexQueryConfig(config));
        }
    }

    private List<SourceConfig> createMultiSource(ReadonlyConfig config) {
        List<Map<String, Object>> configMaps = config.get(SourceConfig.INDEX_LIST);
        List<ReadonlyConfig> configList =
                configMaps.stream().map(ReadonlyConfig::fromMap).collect(Collectors.toList());
        List<SourceConfig> sourceConfigList = new ArrayList<>(configList.size());
        for (ReadonlyConfig readonlyConfig : configList) {
            SourceConfig sourceConfig = parseOneIndexQueryConfig(readonlyConfig);
            sourceConfigList.add(sourceConfig);
        }
        return sourceConfigList;
    }

    private SourceConfig parseOneIndexQueryConfig(ReadonlyConfig readonlyConfig) {

        Map<String, Object> query = readonlyConfig.get(SourceConfig.QUERY);
        String index = readonlyConfig.get(SourceConfig.INDEX);

        CatalogTable catalogTable;
        List<String> source;
        Map<String, String> arrayColumn;

        if (readonlyConfig.getOptional(TableSchemaOptions.SCHEMA).isPresent()) {
            // todo: We need to remove the schema in ES.
            log.warn(
                    "The schema config in ElasticSearch source/sink is deprecated, please use source config instead!");
            catalogTable = CatalogTableUtil.buildWithConfig(readonlyConfig);
            source = Arrays.asList(catalogTable.getSeaTunnelRowType().getFieldNames());
        } else {
            source = readonlyConfig.get(SourceConfig.SOURCE);
            arrayColumn = readonlyConfig.get(SourceConfig.ARRAY_COLUMN);
            Map<String, BasicTypeDefine<EsType>> esFieldType = getFieldTypeMapping(index, source);
            if (CollectionUtils.isEmpty(source)) {
                source = new ArrayList<>(esFieldType.keySet());
            }
            SeaTunnelDataType[] fieldTypes = getSeaTunnelDataType(esFieldType, source);
            TableSchema.Builder builder = TableSchema.builder();

            PkConfig pkConfig = getPkConfig(readonlyConfig);
            builder.primaryKey(
                    PrimaryKey.of(
                            "es_pk",
                            Arrays.asList(pkConfig.getName()),
                            false)); // todo: autoId没有传递到sink

            for (int i = 0; i < source.size(); i++) {
                String key = source.get(i);
                String sourceType = esFieldType.get(key).getDataType();
                if (arrayColumn.containsKey(key)) {
                    String value = arrayColumn.get(key);
                    SeaTunnelDataType<?> dataType =
                            SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(key, value);
                    builder.column(
                            PhysicalColumn.of(
                                    key,
                                    dataType,
                                    0L,
                                    esFieldType.get(key).getScale(),
                                    true,
                                    null,
                                    null,
                                    sourceType,
                                    null));
                    continue;
                }

                builder.column(
                        PhysicalColumn.of(
                                source.get(i),
                                fieldTypes[i],
                                0L,
                                esFieldType.get(key).getScale(),
                                true,
                                null,
                                null,
                                sourceType,
                                null));
            }
            if (!source.contains(pkConfig.getName())) {
                addPkFieldIfNotExistInSourceList(builder, pkConfig);
            }

            catalogTable =
                    CatalogTable.of(
                            TableIdentifier.of("elasticsearch", null, index),
                            builder.build(),
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            "");
        }

        String scrollTime = readonlyConfig.get(SourceConfig.SCROLL_TIME);
        int scrollSize = readonlyConfig.get(SourceConfig.SCROLL_SIZE);
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setSource(source);
        sourceConfig.setCatalogTable(catalogTable);
        sourceConfig.setQuery(query);
        sourceConfig.setScrollTime(scrollTime);
        sourceConfig.setScrollSize(scrollSize);
        sourceConfig.setIndex(index);
        sourceConfig.setCatalogTable(catalogTable);
        return sourceConfig;
    }

    private void addPkFieldIfNotExistInSourceList(TableSchema.Builder builder, PkConfig pkConfig) {
        SeaTunnelDataType<?> dataType = buildPkSeaTunnelDataType(pkConfig);
        builder.column(
                PhysicalColumn.of(
                        pkConfig.getName(),
                        dataType,
                        getColumnLength(pkConfig, pkConfig.getName(), dataType),
                        null,
                        true,
                        null,
                        null));
    }

    private static SeaTunnelDataType<?> buildPkSeaTunnelDataType(PkConfig pkConfig) {
        BasicTypeDefine.BasicTypeDefineBuilder<EsType> typeDefine =
                BasicTypeDefine.<EsType>builder()
                        .name(pkConfig.getName())
                        .columnType(pkConfig.getType())
                        .dataType(pkConfig.getType());
        SeaTunnelDataType<?> dataType =
                ElasticSearchTypeConverter.INSTANCE.convert(typeDefine.build()).getDataType();
        return dataType;
    }

    private long getColumnLength(
            PkConfig pkConfig, String fieldName, SeaTunnelDataType<?> dataType) {
        if (dataType.getSqlType() != SqlType.STRING) {
            return 0;
        }
        // 采用es source这种方式，string类型字段长度统一设为: 2048, 主键如果是string 长度统一为512
        if (pkConfig.getName().equals(fieldName)) {
            return pkConfig.getLength() == null ? STR_PK_DEFAULT_LENGTH : pkConfig.getLength() * 4;
        } else {
            return STR_FIELD_DEFAULT_LENGTH;
        }
    }

    private PkConfig getPkConfig(ReadonlyConfig config) {
        Map pkMap = config.get(SourceConfig.PK);
        Object length = pkMap.get("length");
        return PkConfig.builder()
                .name(pkMap.get("name").toString())
                .type(pkMap.get("type").toString())
                .length(length == null ? null : (int) length)
                .build();
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
        return sourceConfigList.stream()
                .map(SourceConfig::getCatalogTable)
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
                enumeratorContext, connectionConfig, sourceConfigList);
    }

    @Override
    public SourceSplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<ElasticsearchSourceSplit> enumeratorContext,
                    ElasticsearchSourceState sourceState) {
        return new ElasticsearchSourceSplitEnumerator(
                enumeratorContext, sourceState, connectionConfig, sourceConfigList);
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
