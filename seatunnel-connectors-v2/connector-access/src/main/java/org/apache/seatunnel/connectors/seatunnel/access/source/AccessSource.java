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

package org.apache.seatunnel.connectors.seatunnel.access.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.access.client.AccessClient;
import org.apache.seatunnel.connectors.seatunnel.access.config.AccessParameters;
import org.apache.seatunnel.connectors.seatunnel.access.exception.AccessConnectorException;
import org.apache.seatunnel.connectors.seatunnel.access.util.TypeConvertUtil;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.google.auto.service.AutoService;

import java.sql.ResultSetMetaData;

import static org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig.DRIVER;
import static org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig.QUERY;
import static org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig.URL;
import static org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig.USERNAME;

@AutoService(SeaTunnelSource.class)
public class AccessSource extends AbstractSingleSplitSource<SeaTunnelRow>
        implements SupportColumnProjection {
    private SeaTunnelRowType rowTypeInfo;
    private final AccessParameters accessParameters = new AccessParameters();

    @Override
    public String getPluginName() {
        return "Access";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult checkResult =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        DRIVER.key(),
                        URL.key(),
                        USERNAME.key(),
                        PASSWORD.key(),
                        QUERY.key());

        if (!checkResult.isSuccess()) {
            throw new AccessConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, checkResult.getMsg()));
        }

        this.accessParameters.buildWithConfig(pluginConfig);

        AccessClient accessClient =
                new AccessClient(
                        pluginConfig.getString(DRIVER.key()),
                        pluginConfig.getString(URL.key()),
                        pluginConfig.getString(USERNAME.key()),
                        pluginConfig.getString(PASSWORD.key()),
                        pluginConfig.getString(QUERY.key()));
        try {
            ResultSetMetaData metaData = accessClient.selectMetaData();
            int columnSize = metaData.getColumnCount();
            String[] fieldNames = new String[columnSize];
            SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType[columnSize];
            for (int i = 1; i <= columnSize; i++) {
                fieldNames[i - 1] = metaData.getColumnName(i);
                seaTunnelDataTypes[i - 1] = TypeConvertUtil.convert(metaData.getColumnTypeName(i));
            }
            this.rowTypeInfo = new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
        } catch (Exception e) {
            throw new RuntimeException(e);
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
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new AccessSourceReader(accessParameters, readerContext);
    }
}
