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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceOptions.LabelType;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.structure.schema.PropertyKey;
import org.apache.hugegraph.structure.schema.VertexLabel;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@AutoService(Factory.class)
public class HugeGraphSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return HugeGraphOptions.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        HugeGraphOptions.HOST,
                        HugeGraphOptions.PORT,
                        HugeGraphOptions.GRAPH_NAME,
                        HugeGraphSourceOptions.LABEL,
                        HugeGraphSourceOptions.TYPE)
                .optional(
                        HugeGraphOptions.GRAPH_SPACE,
                        HugeGraphOptions.USERNAME,
                        HugeGraphOptions.PASSWORD,
                        HugeGraphOptions.PROTOCOL,
                        HugeGraphSourceOptions.PROPERTIES,
                        HugeGraphSourceOptions.PAGE_SIZE,
                        HugeGraphSourceOptions.LIMIT,
                        ConnectorCommonOptions.SCHEMA)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return HugeGraphSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        HugeGraphSourceConfig sourceConfig = HugeGraphSourceConfig.of(context.getOptions());

        CatalogTable catalogTable;
        if (context.getOptions().getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            catalogTable = CatalogTableUtil.buildWithConfig(context.getOptions());
        } else {
            catalogTable = inferCatalogTable(sourceConfig);
        }

        final CatalogTable finalCatalogTable = catalogTable;
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new HugeGraphSource(sourceConfig, finalCatalogTable);
    }

    private CatalogTable inferCatalogTable(HugeGraphSourceConfig sourceConfig) {
        HugeGraphClient client =
                new HugeGraphClient(
                        sourceConfig.getHost(),
                        sourceConfig.getPort(),
                        sourceConfig.getGraphName(),
                        sourceConfig.getGraphSpace(),
                        sourceConfig.getUsername(),
                        sourceConfig.getPassword(),
                        sourceConfig.getMaxRetries(),
                        sourceConfig.getRetryBackoffMs(),
                        sourceConfig.getProtocol());

        try {
            SeaTunnelRowType rowType;
            if (sourceConfig.getType() == LabelType.VERTEX) {
                rowType = inferVertexRowType(client, sourceConfig);
            } else {
                rowType = inferEdgeRowType(client, sourceConfig);
            }

            // Build table schema from row type
            List<org.apache.seatunnel.api.table.catalog.Column> columns = new ArrayList<>();
            for (int i = 0; i < rowType.getTotalFields(); i++) {
                columns.add(
                        PhysicalColumn.builder()
                                .name(rowType.getFieldName(i))
                                .dataType(rowType.getFieldType(i))
                                .nullable(true)
                                .build());
            }

            TableSchema tableSchema = TableSchema.builder().columns(columns).build();

            return CatalogTable.of(
                    TableIdentifier.of("default", "default", "default"),
                    tableSchema,
                    new HashMap<>(),
                    new ArrayList<>(),
                    "Inferred from HugeGraph schema");
        } finally {
            client.close();
        }
    }

    private SeaTunnelRowType inferVertexRowType(
            HugeGraphClient client, HugeGraphSourceConfig sourceConfig) {
        VertexLabel vertexLabel = client.getVertexLabel(sourceConfig.getLabel());
        List<String> fieldNames = new ArrayList<>();
        List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>();

        fieldNames.add("id");
        fieldTypes.add(BasicType.STRING_TYPE);

        fieldNames.add("label");
        fieldTypes.add(BasicType.STRING_TYPE);

        for (String propertyName : vertexLabel.properties()) {
            PropertyKey propertyKey = client.getPropertyKey(propertyName);
            fieldNames.add(propertyName);
            fieldTypes.add(inferPropertyType(propertyKey));
        }

        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]), fieldTypes.toArray(new SeaTunnelDataType[0]));
    }

    private SeaTunnelRowType inferEdgeRowType(
            HugeGraphClient client, HugeGraphSourceConfig sourceConfig) {
        EdgeLabel edgeLabel = client.getEdgeLabel(sourceConfig.getLabel());
        List<String> fieldNames = new ArrayList<>();
        List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>();

        fieldNames.add("id");
        fieldTypes.add(BasicType.STRING_TYPE);

        fieldNames.add("label");
        fieldTypes.add(BasicType.STRING_TYPE);

        fieldNames.add("source_id");
        fieldTypes.add(BasicType.STRING_TYPE);

        fieldNames.add("target_id");
        fieldTypes.add(BasicType.STRING_TYPE);

        for (String propertyName : edgeLabel.properties()) {
            PropertyKey propertyKey = client.getPropertyKey(propertyName);
            fieldNames.add(propertyName);
            fieldTypes.add(inferPropertyType(propertyKey));
        }

        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]), fieldTypes.toArray(new SeaTunnelDataType[0]));
    }

    private SeaTunnelDataType<?> inferPropertyType(PropertyKey propertyKey) {
        SeaTunnelDataType<?> baseType = mapDataType(propertyKey.dataType());
        if (propertyKey.cardinality() == Cardinality.LIST
                || propertyKey.cardinality() == Cardinality.SET) {
            return ArrayType.of(baseType);
        }
        return baseType;
    }

    private SeaTunnelDataType<?> mapDataType(DataType hugeGraphType) {
        switch (hugeGraphType) {
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case INT:
                return BasicType.INT_TYPE;
            case LONG:
                return BasicType.LONG_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case UUID:
                return BasicType.STRING_TYPE;
            case TEXT:
            default:
                return BasicType.STRING_TYPE;
        }
    }
}
