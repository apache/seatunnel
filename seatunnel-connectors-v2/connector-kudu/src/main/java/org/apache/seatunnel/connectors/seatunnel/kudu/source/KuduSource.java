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

package org.apache.seatunnel.connectors.seatunnel.kudu.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient.KuduInputFormat;
import org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient.KuduTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.kudu.state.KuduSourceState;
import org.apache.seatunnel.connectors.seatunnel.kudu.util.KuduUtil;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class KuduSource
        implements SeaTunnelSource<SeaTunnelRow, KuduSourceSplit, KuduSourceState>,
                SupportParallelism {

    private SeaTunnelRowType rowTypeInfo;
    private KuduInputFormat kuduInputFormat;
    private KuduSourceConfig kuduSourceConfig;

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, KuduSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new KuduSourceReader(kuduInputFormat, readerContext);
    }

    @Override
    public SourceSplitEnumerator<KuduSourceSplit, KuduSourceState> createEnumerator(
            SourceSplitEnumerator.Context<KuduSourceSplit> enumeratorContext) {
        return new KuduSourceSplitEnumerator(enumeratorContext, kuduSourceConfig, kuduInputFormat);
    }

    @Override
    public SourceSplitEnumerator<KuduSourceSplit, KuduSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<KuduSourceSplit> enumeratorContext,
            KuduSourceState checkpointState) {
        return new KuduSourceSplitEnumerator(
                enumeratorContext, kuduSourceConfig, kuduInputFormat, checkpointState);
    }

    @Override
    public String getPluginName() {
        return "Kudu";
    }

    @Override
    public void prepare(Config pluginConfig) {
        ReadonlyConfig config = ReadonlyConfig.fromConfig(pluginConfig);
        ConfigValidator.of(config).validate(new KuduSourceFactory().optionRule());
        try {
            kuduSourceConfig = new KuduSourceConfig(config);
        } catch (Exception e) {
            throw new KuduConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, ExceptionUtils.getMessage(e)));
        }

        if (pluginConfig.hasPath(TableSchemaOptions.SCHEMA.key())) {
            rowTypeInfo = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
        } else {
            try (KuduClient kuduClient = KuduUtil.getKuduClient(kuduSourceConfig)) {
                rowTypeInfo =
                        getSeaTunnelRowType(
                                kuduClient
                                        .openTable(kuduSourceConfig.getTable())
                                        .getSchema()
                                        .getColumns());
            } catch (Exception e) {
                throw new KuduConnectorException(KuduConnectorErrorCode.INIT_KUDU_CLIENT_FAILED, e);
            }
        }
        kuduInputFormat = new KuduInputFormat(kuduSourceConfig, rowTypeInfo);
    }

    public SeaTunnelRowType getSeaTunnelRowType(List<ColumnSchema> columnSchemaList) {
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {

            for (int i = 0; i < columnSchemaList.size(); i++) {
                fieldNames.add(columnSchemaList.get(i).getName());
                seaTunnelDataTypes.add(KuduTypeMapper.mapping(columnSchemaList, i));
            }

        } catch (Exception e) {
            throw new KuduConnectorException(
                    CommonErrorCode.TABLE_SCHEMA_GET_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            "Kudu", PluginType.SOURCE, ExceptionUtils.getMessage(e)));
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]),
                seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }
}
