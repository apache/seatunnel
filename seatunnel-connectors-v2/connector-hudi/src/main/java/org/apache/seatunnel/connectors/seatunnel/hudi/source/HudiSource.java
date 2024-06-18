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

package org.apache.seatunnel.connectors.seatunnel.hudi.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiError;
import org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil;

import com.google.auto.service.AutoService;

import java.io.IOException;

import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSourceConfig.CONF_FILES;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSourceConfig.KERBEROS_PRINCIPAL;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSourceConfig.KERBEROS_PRINCIPAL_FILE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSourceConfig.TABLE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSourceConfig.TABLE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSourceConfig.USE_KERBEROS;

@AutoService(SeaTunnelSource.class)
public class HudiSource
        implements SeaTunnelSource<SeaTunnelRow, HudiSourceSplit, HudiSourceState>,
                SupportParallelism {

    private SeaTunnelRowType typeInfo;

    private String filePath;

    private String tablePath;

    private String confFiles;

    private boolean useKerberos = false;

    @Override
    public String getPluginName() {
        return "Hudi";
    }

    @Override
    public void prepare(Config pluginConfig) {
        CheckResult result =
                CheckConfigUtil.checkAllExists(pluginConfig, TABLE_PATH.key(), CONF_FILES.key());
        if (!result.isSuccess()) {
            throw new HudiConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        // default hudi table type is cow
        // TODO: support hudi mor table
        // TODO: support Incremental Query and Read Optimized Query
        if (!"cow".equalsIgnoreCase(pluginConfig.getString(TABLE_TYPE.key()))) {
            throw new HudiConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(),
                            PluginType.SOURCE,
                            "Do not support hudi mor table yet!"));
        }
        try {
            this.confFiles = pluginConfig.getString(CONF_FILES.key());
            this.tablePath = pluginConfig.getString(TABLE_PATH.key());
            if (CheckConfigUtil.isValidParam(pluginConfig, USE_KERBEROS.key())) {
                this.useKerberos = pluginConfig.getBoolean(USE_KERBEROS.key());
                if (this.useKerberos) {
                    CheckResult kerberosCheckResult =
                            CheckConfigUtil.checkAllExists(
                                    pluginConfig,
                                    KERBEROS_PRINCIPAL.key(),
                                    KERBEROS_PRINCIPAL_FILE.key());
                    if (!kerberosCheckResult.isSuccess()) {
                        throw new HudiConnectorException(
                                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                                String.format(
                                        "PluginName: %s, PluginType: %s, Message: %s",
                                        getPluginName(), PluginType.SOURCE, result.getMsg()));
                    }
                    HudiUtil.initKerberosAuthentication(
                            HudiUtil.getConfiguration(this.confFiles),
                            pluginConfig.getString(KERBEROS_PRINCIPAL.key()),
                            pluginConfig.getString(KERBEROS_PRINCIPAL_FILE.key()));
                }
            }
            this.filePath = HudiUtil.getParquetFileByPath(this.confFiles, tablePath);
            if (this.filePath == null) {
                throw HudiError.cannotFindParquetFile(tablePath);
            }
            // should read from config or read from hudi metadata( wait catalog done)
            this.typeInfo = HudiUtil.getSeaTunnelRowTypeInfo(this.confFiles, this.filePath);
        } catch (HudiConnectorException | IOException e) {
            throw new HudiConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, HudiSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new HudiSourceReader(this.confFiles, readerContext, typeInfo);
    }

    @Override
    public Boundedness getBoundedness() {
        //  Only support Snapshot Query now.
        //  After support Incremental Query and Read Optimized Query, we should supoort UNBOUNDED.
        //  TODO: support UNBOUNDED
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceSplitEnumerator<HudiSourceSplit, HudiSourceState> createEnumerator(
            SourceSplitEnumerator.Context<HudiSourceSplit> enumeratorContext) throws Exception {
        return new HudiSourceSplitEnumerator(enumeratorContext, tablePath, this.confFiles);
    }

    @Override
    public SourceSplitEnumerator<HudiSourceSplit, HudiSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<HudiSourceSplit> enumeratorContext,
            HudiSourceState checkpointState)
            throws Exception {
        return new HudiSourceSplitEnumerator(
                enumeratorContext, tablePath, this.confFiles, checkpointState);
    }
}
