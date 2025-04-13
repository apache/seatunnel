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

package org.apache.seatunnel.connectors.seatunnel.easysearch.source;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.easysearch.catalog.EasysearchDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.easysearch.client.EasysearchClient;
import org.apache.seatunnel.connectors.seatunnel.easysearch.config.EasysearchSourceOptions;

import org.apache.commons.collections4.CollectionUtils;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@AutoService(Factory.class)
public class EasysearchSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Easysearch";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(EasysearchSourceOptions.HOSTS, EasysearchSourceOptions.INDEX)
                .optional(
                        EasysearchSourceOptions.USERNAME,
                        EasysearchSourceOptions.PASSWORD,
                        EasysearchSourceOptions.SCROLL_TIME,
                        EasysearchSourceOptions.SCROLL_SIZE,
                        EasysearchSourceOptions.QUERY,
                        EasysearchSourceOptions.TLS_VERIFY_CERTIFICATE,
                        EasysearchSourceOptions.TLS_VERIFY_HOSTNAME,
                        EasysearchSourceOptions.TLS_KEY_STORE_PATH,
                        EasysearchSourceOptions.TLS_KEY_STORE_PASSWORD,
                        EasysearchSourceOptions.TLS_TRUST_STORE_PATH,
                        EasysearchSourceOptions.TLS_TRUST_STORE_PASSWORD)
                .exclusive(EasysearchSourceOptions.SOURCE, ConnectorCommonOptions.SCHEMA)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig contextOptions = context.getOptions();
        List<String> source;
        CatalogTable catalogTable;
        if (contextOptions.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            // todo: We need to remove the schema in EZS.
            catalogTable = CatalogTableUtil.buildWithConfig(contextOptions);
            source =
                    Arrays.asList(
                            CatalogTableUtil.buildWithConfig(contextOptions)
                                    .getSeaTunnelRowType()
                                    .getFieldNames());
        } else {
            if (contextOptions.getOptional(EasysearchSourceOptions.SOURCE).isPresent()) {
                source = contextOptions.get(EasysearchSourceOptions.SOURCE);
            } else {
                source = Lists.newArrayList();
            }
            EasysearchClient ezsClient = EasysearchClient.createInstance(contextOptions);
            Map<String, String> ezsFieldType =
                    ezsClient.getFieldTypeMapping(
                            contextOptions.get(EasysearchSourceOptions.INDEX), source);
            ezsClient.close();
            EasysearchDataTypeConvertor easySearchDataTypeConvertor =
                    new EasysearchDataTypeConvertor();
            List<Column> columns = new ArrayList<>();
            if (CollectionUtils.isEmpty(source)) {
                List<String> keys = new ArrayList<>(ezsFieldType.keySet());
                for (int i = 0; i < keys.size(); i++) {
                    String esType = ezsFieldType.get(keys.get(i));
                    PhysicalColumn physicalColumn =
                            PhysicalColumn.of(
                                    keys.get(i),
                                    easySearchDataTypeConvertor.toSeaTunnelType(
                                            keys.get(i), esType),
                                    null,
                                    null,
                                    true,
                                    null,
                                    null);
                    columns.add(physicalColumn);
                }
            } else {
                for (int i = 0; i < source.size(); i++) {
                    String esType = ezsFieldType.get(source.get(i));
                    PhysicalColumn physicalColumn =
                            PhysicalColumn.of(
                                    source.get(i),
                                    easySearchDataTypeConvertor.toSeaTunnelType(
                                            source.get(i), esType),
                                    null,
                                    null,
                                    true,
                                    null,
                                    null);
                    columns.add(physicalColumn);
                }
            }
            catalogTable =
                    CatalogTable.of(
                            TableIdentifier.of("default", "default", "default"),
                            TableSchema.builder().columns(columns).build(),
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            "");
        }

        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new EasysearchSource(contextOptions, source, catalogTable);
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return EasysearchSource.class;
    }
}
