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
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.easysearch.catalog.EasysearchDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.easysearch.client.EasysearchClient;
import org.apache.seatunnel.connectors.seatunnel.easysearch.config.SourceConfig;

import org.apache.commons.collections4.CollectionUtils;

import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@AutoService(SeaTunnelSource.class)
public class EasysearchSource
        implements SeaTunnelSource<SeaTunnelRow, EasysearchSourceSplit, EasysearchSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private Config pluginConfig;

    private SeaTunnelRowType rowTypeInfo;

    private List<String> source;

    @Override
    public String getPluginName() {
        return "Easysearch";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        if (pluginConfig.hasPath(ConnectorCommonOptions.SCHEMA.key())) {
            // todo: We need to remove the schema in EZS.
            rowTypeInfo = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
            source = Arrays.asList(rowTypeInfo.getFieldNames());
        } else {
            if (pluginConfig.hasPath(SourceConfig.SOURCE.key())) {
                source = pluginConfig.getStringList(SourceConfig.SOURCE.key());
            } else {
                source = Lists.newArrayList();
            }
            EasysearchClient ezsClient = EasysearchClient.createInstance(this.pluginConfig);
            Map<String, String> ezsFieldType =
                    ezsClient.getFieldTypeMapping(
                            pluginConfig.getString(SourceConfig.INDEX.key()), source);
            ezsClient.close();
            EasysearchDataTypeConvertor easySearchDataTypeConvertor =
                    new EasysearchDataTypeConvertor();
            if (CollectionUtils.isEmpty(source)) {
                List<String> keys = new ArrayList<>(ezsFieldType.keySet());
                SeaTunnelDataType[] fieldTypes = new SeaTunnelDataType[keys.size()];
                for (int i = 0; i < keys.size(); i++) {
                    String esType = ezsFieldType.get(keys.get(i));
                    SeaTunnelDataType seaTunnelDataType =
                            easySearchDataTypeConvertor.toSeaTunnelType(keys.get(i), esType);
                    fieldTypes[i] = seaTunnelDataType;
                }
                rowTypeInfo = new SeaTunnelRowType(keys.toArray(new String[0]), fieldTypes);
            } else {
                SeaTunnelDataType[] fieldTypes = new SeaTunnelDataType[source.size()];
                for (int i = 0; i < source.size(); i++) {
                    String esType = ezsFieldType.get(source.get(i));
                    SeaTunnelDataType seaTunnelDataType =
                            easySearchDataTypeConvertor.toSeaTunnelType(source.get(i), esType);
                    fieldTypes[i] = seaTunnelDataType;
                }
                rowTypeInfo = new SeaTunnelRowType(source.toArray(new String[0]), fieldTypes);
            }
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, EasysearchSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new EasysearchSourceReader(readerContext, pluginConfig, rowTypeInfo);
    }

    @Override
    public SourceSplitEnumerator<EasysearchSourceSplit, EasysearchSourceState> createEnumerator(
            SourceSplitEnumerator.Context<EasysearchSourceSplit> enumeratorContext) {
        return new EasysearchSourceSplitEnumerator(enumeratorContext, pluginConfig, source);
    }

    @Override
    public SourceSplitEnumerator<EasysearchSourceSplit, EasysearchSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<EasysearchSourceSplit> enumeratorContext,
            EasysearchSourceState sourceState) {
        return new EasysearchSourceSplitEnumerator(
                enumeratorContext, sourceState, pluginConfig, source);
    }
}
