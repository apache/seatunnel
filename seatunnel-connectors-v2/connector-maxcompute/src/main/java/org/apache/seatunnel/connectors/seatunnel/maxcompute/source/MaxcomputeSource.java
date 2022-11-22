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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import com.aliyun.odps.PartitionSpec;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.client.*;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.maxcomputeclient.MaxcomputeInputFormat;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class MaxcomputeSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private SeaTunnelRowType rowTypeInfo;
    private MaxcomputeInputFormat kuduInputFormat;
    private PartitionSpec partitionParameter;
    public static final int TIMEOUTMS = 18000;

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public String getPluginName() {
        return "MaxcomputeSource";
    }

    @Override
    public void prepare(Config config) {
        String aliyunOdpsAccountid = "";
        String aliyunOdpsAccountkey = "";
        String aliyunOdpsEndpoint = "";
        String aliyunOdpsDefaultProject = "";
        String maxcomputeTablePartition = "";
        String maxcomputeTableName = "";
        if (config.hasPath(MaxcomputeSourceConfig.ALIYUN_ODPS_ACCOUNTID.key())
                && config.hasPath(MaxcomputeSourceConfig.ALIYUN_ODPS_ACCOUNTKEY.key())
                && config.hasPath(MaxcomputeSourceConfig.ALIYUN_ODPS_ENDPOINT.key())
                && config.hasPath(MaxcomputeSourceConfig.ALIYUN_ODPS_DEFAULTPROJECT.key())
                && config.hasPath(MaxcomputeSourceConfig.MAXCOMPUTE_TABLE_PARTITION.key())
                && config.hasPath(MaxcomputeSourceConfig.MAXCOMPUTE_TABLE_NAME.key())) {
            aliyunOdpsAccountid = config.getString(MaxcomputeSourceConfig.ALIYUN_ODPS_ACCOUNTID.key());
            aliyunOdpsAccountkey = config.getString(MaxcomputeSourceConfig.ALIYUN_ODPS_ACCOUNTKEY.key());
            aliyunOdpsEndpoint = config.getString(MaxcomputeSourceConfig.ALIYUN_ODPS_ENDPOINT.key());
            aliyunOdpsDefaultProject = config.getString(MaxcomputeSourceConfig.ALIYUN_ODPS_DEFAULTPROJECT.key());
            maxcomputeTablePartition = config.getString(MaxcomputeSourceConfig.MAXCOMPUTE_TABLE_PARTITION.key());
            maxcomputeTableName = config.getString(MaxcomputeSourceConfig.MAXCOMPUTE_TABLE_NAME.key());
        } else {
            throw new RuntimeException("Missing Source configuration parameters");
        }
        try {
            kuduInputFormat = new MaxcomputeInputFormat(aliyunOdpsAccountid, aliyunOdpsAccountkey, aliyunOdpsEndpoint
                    , aliyunOdpsDefaultProject, maxcomputeTablePartition, maxcomputeTableName);
        } catch (Exception e) {
            throw new RuntimeException("Parameters in the preparation phase fail to be generated", e);
        }
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        return new MaxcomputeSourceReader(kuduInputFormat,readerContext);
    }
}
