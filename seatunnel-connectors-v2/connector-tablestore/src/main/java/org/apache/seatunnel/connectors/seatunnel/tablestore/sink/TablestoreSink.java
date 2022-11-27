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

package org.apache.seatunnel.connectors.seatunnel.tablestore.sink;

import static org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreConfig.ACCESS_KEY_ID;
import static org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreConfig.ACCESS_KEY_SECRET;
import static org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreConfig.END_POINT;
import static org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreConfig.INSTANCE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreConfig.PRIMARY_KEYS;
import static org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreConfig.TABLE;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreOptions;
import org.apache.seatunnel.connectors.seatunnel.tablestore.exception.TablestoreConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;

@AutoService(SeaTunnelSink.class)
public class TablestoreSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private SeaTunnelRowType rowType;

    private TablestoreOptions tablestoreOptions;

    @Override
    public String getPluginName() {
        return "Tablestore";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, END_POINT.key(), TABLE.key(), INSTANCE_NAME.key(), ACCESS_KEY_ID.key(), ACCESS_KEY_SECRET.key(), PRIMARY_KEYS.key());
        if (!result.isSuccess()) {
            throw new TablestoreConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        tablestoreOptions = new TablestoreOptions(pluginConfig);
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.rowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return rowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        return new TablestoreWriter(tablestoreOptions, rowType);
    }
}
