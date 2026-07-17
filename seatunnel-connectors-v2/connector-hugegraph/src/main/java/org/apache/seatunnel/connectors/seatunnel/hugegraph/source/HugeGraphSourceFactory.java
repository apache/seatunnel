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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphOperations;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.ReservedColumns;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.HugeGraphTypeConverter;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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
                        HugeGraphSourceOptions.LABEL)
                .optional(
                        // Optional: when omitted, the property columns are auto-discovered from the
                        // server label definition (all properties, inferred types).
                        ConnectorCommonOptions.SCHEMA,
                        HugeGraphSourceOptions.LABEL_TYPE,
                        HugeGraphSourceOptions.PAGE_SIZE,
                        HugeGraphSourceOptions.SPLIT_SIZE,
                        HugeGraphSourceOptions.FILTER,
                        HugeGraphSourceOptions.TIME_ZONE,
                        HugeGraphOptions.PROTOCOL,
                        HugeGraphOptions.USERNAME,
                        HugeGraphOptions.PASSWORD,
                        // Optional connection setting passed through to select the HugeGraph graph
                        // space (defaults to "DEFAULT").
                        HugeGraphOptions.GRAPH_SPACE,
                        HugeGraphOptions.MAX_RETRIES,
                        HugeGraphOptions.RETRY_BACKOFF_MS,
                        HugeGraphOptions.RETRY_BACKOFF_MAX_MS)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return HugeGraphSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        checkFilterParallelism(options);
        MappingConfig.LabelType labelType =
                options.getOptional(HugeGraphSourceOptions.LABEL_TYPE)
                        .orElse(HugeGraphSourceOptions.LABEL_TYPE.defaultValue());
        CatalogTable propertyCatalogTable = resolvePropertyCatalogTable(options, labelType);
        SeaTunnelRowType propertyRowType = propertyCatalogTable.getSeaTunnelRowType();
        HugeGraphSourceConfig sourceConfig = HugeGraphSourceConfig.of(options, propertyRowType);
        CatalogTable producedCatalogTable =
                CatalogTableUtil.newCatalogTable(
                        propertyCatalogTable,
                        prependReservedFields(propertyRowType, sourceConfig.getLabelType()));
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new HugeGraphSource(producedCatalogTable, sourceConfig);
    }

    /**
     * Resolves the property columns. When {@code schema} is configured it is used verbatim;
     * otherwise the columns are auto-discovered from the server label definition (all property
     * keys, with types inferred from the server).
     */
    private CatalogTable resolvePropertyCatalogTable(
            ReadonlyConfig options, MappingConfig.LabelType labelType) {
        if (options.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            return CatalogTableUtil.buildWithConfig(options);
        }
        String label = options.get(HugeGraphSourceOptions.LABEL);
        HugeGraphClient client = new HugeGraphClient(HugeGraphConnectionConfig.of(options));
        SeaTunnelRowType propertyRowType;
        try {
            propertyRowType = discoverPropertyRowType(client, label, labelType);
        } finally {
            client.close();
        }
        return CatalogTableUtil.getCatalogTable(label, propertyRowType);
    }

    /**
     * Builds the property row type from the server label definition: every property key of the
     * label, ordered by name for a deterministic column order, typed via {@link
     * HugeGraphTypeConverter}.
     */
    static SeaTunnelRowType discoverPropertyRowType(
            HugeGraphOperations client, String label, MappingConfig.LabelType labelType) {
        Set<String> properties =
                labelType == MappingConfig.LabelType.VERTEX
                        ? client.getVertexLabelPropertiesOrNull(label)
                        : client.getEdgeLabelPropertiesOrNull(label);
        if (properties == null) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "%s label '%s' does not exist in HugeGraph schema; cannot auto-discover "
                                    + "its columns. Create the label or declare 'schema.fields'.",
                            labelType == MappingConfig.LabelType.VERTEX ? "Vertex" : "Edge",
                            label));
        }
        List<String> names = new ArrayList<>(properties);
        Collections.sort(names);
        String[] fieldNames = new String[names.size()];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType<?>[names.size()];
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            fieldNames[i] = name;
            fieldTypes[i] =
                    HugeGraphTypeConverter.toSeaTunnelType(
                            client.getPropertyDataType(name),
                            client.getPropertyCardinality(name),
                            name);
        }
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    /**
     * Parallelism &gt; 1 uses shard-based key-range scans, which cannot push a property-equality
     * {@code filter} to the server (the scan API takes no condition). Reject that combination here,
     * at plan/config time, so the user gets an actionable choice before the job starts rather than
     * a silently-ignored filter or a mid-run failure. {@code filter} with parallelism = 1
     * (label-list scan) is fully supported.
     */
    static void checkFilterParallelism(ReadonlyConfig options) {
        int parallelism = options.getOptional(EnvCommonOptions.PARALLELISM).orElse(1);
        boolean hasFilter =
                options.getOptional(HugeGraphSourceOptions.FILTER)
                        .map(filter -> !filter.isEmpty())
                        .orElse(false);
        if (parallelism > 1 && hasFilter) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "HugeGraph source 'filter' cannot be combined with parallelism > 1 "
                                    + "(got %d): parallel reads use shard key-range scans that do not "
                                    + "support server-side property filtering. Either set parallelism "
                                    + "to 1 to keep the filter, or remove the filter to read in "
                                    + "parallel.",
                            parallelism));
        }
    }

    static SeaTunnelRowType prependReservedFields(
            SeaTunnelRowType propertyRowType, MappingConfig.LabelType labelType) {
        // The source auto-prepends reserved columns (~id/~label, plus edge endpoints). A user
        // schema.fields column with a reserved name would silently create a duplicate column and
        // later fail with a misleading "label has no property ~id"; reject it up front with a clear
        // message instead. Auto-discovered names never start with '~' (HugeGraph forbids it), so
        // this only triggers on an explicit schema.fields declaration.
        for (String fieldName : propertyRowType.getFieldNames()) {
            if (ReservedColumns.isReserved(fieldName)) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        String.format(
                                "schema.fields must not declare the reserved column '%s': names "
                                        + "starting with '%s' are emitted automatically by the "
                                        + "HugeGraph source (%s for vertices; also %s, %s, %s, %s "
                                        + "for edges). Remove '%s' from schema.fields.",
                                fieldName,
                                ReservedColumns.PREFIX,
                                ReservedColumns.ID + "/" + ReservedColumns.LABEL,
                                ReservedColumns.SOURCE_ID,
                                ReservedColumns.SOURCE_LABEL,
                                ReservedColumns.TARGET_ID,
                                ReservedColumns.TARGET_LABEL,
                                fieldName));
            }
        }
        int reservedSize = labelType == MappingConfig.LabelType.VERTEX ? 2 : 6;
        String[] fieldNames = new String[reservedSize + propertyRowType.getTotalFields()];
        SeaTunnelDataType<?>[] fieldTypes =
                new SeaTunnelDataType<?>[reservedSize + propertyRowType.getTotalFields()];

        fieldNames[0] = HugeGraphSourceReader.ID_FIELD;
        fieldNames[1] = HugeGraphSourceReader.LABEL_FIELD;
        fieldTypes[0] = BasicType.STRING_TYPE;
        fieldTypes[1] = BasicType.STRING_TYPE;
        if (labelType == MappingConfig.LabelType.EDGE) {
            fieldNames[2] = HugeGraphSourceReader.SOURCE_ID_FIELD;
            fieldNames[3] = HugeGraphSourceReader.SOURCE_LABEL_FIELD;
            fieldNames[4] = HugeGraphSourceReader.TARGET_ID_FIELD;
            fieldNames[5] = HugeGraphSourceReader.TARGET_LABEL_FIELD;
            for (int i = 2; i < reservedSize; i++) {
                fieldTypes[i] = BasicType.STRING_TYPE;
            }
        }

        for (int i = 0; i < propertyRowType.getTotalFields(); i++) {
            fieldNames[reservedSize + i] = propertyRowType.getFieldName(i);
            fieldTypes[reservedSize + i] = propertyRowType.getFieldType(i);
        }
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }
}
