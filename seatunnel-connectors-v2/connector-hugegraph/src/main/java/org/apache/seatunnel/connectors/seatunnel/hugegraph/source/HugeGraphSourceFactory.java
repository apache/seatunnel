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
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import com.google.auto.service.AutoService;

import java.io.Serializable;

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
                        ConnectorCommonOptions.SCHEMA)
                .optional(
                        HugeGraphSourceOptions.LABEL_TYPE,
                        HugeGraphSourceOptions.PAGE_SIZE,
                        HugeGraphSourceOptions.TIME_ZONE,
                        HugeGraphOptions.PROTOCOL,
                        HugeGraphOptions.USERNAME,
                        HugeGraphOptions.PASSWORD,
                        // Accepted only so the shared connection config can emit the actionable
                        // migration error; leaving it out makes ConfigValidator reject it as an
                        // unknown option first, hiding that message.
                        HugeGraphOptions.GRAPH_SPACE,
                        HugeGraphOptions.MAX_RETRIES,
                        HugeGraphOptions.RETRY_BACKOFF_MS)
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
        checkParallelism(options);
        CatalogTable propertyCatalogTable = CatalogTableUtil.buildWithConfig(options);
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
     * The HugeGraph source is single-split: AbstractSingleSplitSource allows only the subtask-0
     * reader and throws at runtime for any higher subtask index. Reject {@code parallelism > 1}
     * here, at plan/config time, so the user gets an actionable error before the job starts instead
     * of a mid-run failure.
     */
    static void checkParallelism(ReadonlyConfig options) {
        int parallelism = options.getOptional(EnvCommonOptions.PARALLELISM).orElse(1);
        if (parallelism > 1) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "HugeGraph source is single-split and supports only parallelism = 1, "
                                    + "but got %d. Remove the source 'parallelism' option or set it to 1.",
                            parallelism));
        }
    }

    static SeaTunnelRowType prependReservedFields(
            SeaTunnelRowType propertyRowType, MappingConfig.LabelType labelType) {
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
