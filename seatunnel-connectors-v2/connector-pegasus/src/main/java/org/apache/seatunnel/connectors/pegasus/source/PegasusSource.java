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

package org.apache.seatunnel.connectors.pegasus.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class PegasusSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private static final String[] fieldNames = {"hash_key", "sort_key", "value"};

    private static final PrimitiveByteArrayType[] fieldTypes =
            new PrimitiveByteArrayType[] {
                PrimitiveByteArrayType.INSTANCE,
                PrimitiveByteArrayType.INSTANCE,
                PrimitiveByteArrayType.INSTANCE
            };

    private Config pluginConfig;

    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public String getPluginName() {
        return "Pegasus";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        PegasusSourceConfig.META_SERVER.key(),
                        PegasusSourceConfig.TABLE.key(),
                        PegasusSourceConfig.READ_MODE.key());
        if (!result.isSuccess()) {
            throw new PrepareFailException("Pegasus", PluginType.SOURCE, result.getMsg());
        }
        this.pluginConfig = pluginConfig;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        PegasusSourceConfig.ReadMode readMode =
                pluginConfig.getEnum(
                        PegasusSourceConfig.ReadMode.class, PegasusSourceConfig.READ_MODE.key());
        switch (readMode) {
            case unorderedScanner:
                return new PegasusSourceScannerReader(pluginConfig, readerContext);
        }
        throw new UnsupportedOperationException("read mode doesn't support: " + readMode);
    }
}
