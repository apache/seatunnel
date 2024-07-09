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

package org.apache.seatunnel.connectors.seatunnel.hbase.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorException;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.FAMILY_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ROWKEY_COLUMNS;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ZOOKEEPER_QUORUM;

@AutoService(SeaTunnelSink.class)
public class HbaseSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private Config pluginConfig;

    private SeaTunnelRowType seaTunnelRowType;

    private HbaseParameters hbaseParameters;

    private List<Integer> rowkeyColumnIndexes = new ArrayList<>();

    private int versionColumnIndex = -1;

    @Override
    public String getPluginName() {
        return HbaseSinkFactory.IDENTIFIER;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        ZOOKEEPER_QUORUM.key(),
                        TABLE.key(),
                        ROWKEY_COLUMNS.key(),
                        FAMILY_NAME.key());
        if (!result.isSuccess()) {
            throw new HbaseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        this.hbaseParameters = HbaseParameters.buildWithConfig(pluginConfig);
        if (hbaseParameters.getFamilyNames().size() == 0) {
            throw new HbaseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "The corresponding field options should be configured and should not be empty Refer to the hbase sink document");
        }
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
        for (String rowkeyColumn : hbaseParameters.getRowkeyColumns()) {
            this.rowkeyColumnIndexes.add(seaTunnelRowType.indexOf(rowkeyColumn));
        }
        if (hbaseParameters.getVersionColumn() != null) {
            this.versionColumnIndex = seaTunnelRowType.indexOf(hbaseParameters.getVersionColumn());
        }
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new HbaseSinkWriter(
                seaTunnelRowType, hbaseParameters, rowkeyColumnIndexes, versionColumnIndex);
    }
}
