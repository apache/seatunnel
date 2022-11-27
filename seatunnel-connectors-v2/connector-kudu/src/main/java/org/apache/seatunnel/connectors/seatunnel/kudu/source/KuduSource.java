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

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient.KuduInputFormat;
import org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient.KuduTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.kudu.state.KuduSourceState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class KuduSource implements SeaTunnelSource<SeaTunnelRow, KuduSourceSplit, KuduSourceState> {
    private SeaTunnelRowType rowTypeInfo;
    private KuduInputFormat kuduInputFormat;
    private PartitionParameter partitionParameter;
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
    public SourceReader<SeaTunnelRow, KuduSourceSplit> createReader(SourceReader.Context readerContext) {
        return new KuduSourceReader(kuduInputFormat, readerContext);
    }

    @Override
    public Serializer<KuduSourceSplit> getSplitSerializer() {
        return SeaTunnelSource.super.getSplitSerializer();
    }

    @Override
    public SourceSplitEnumerator<KuduSourceSplit, KuduSourceState> createEnumerator(
            SourceSplitEnumerator.Context<KuduSourceSplit> enumeratorContext) {
        return new KuduSourceSplitEnumerator(enumeratorContext, partitionParameter);
    }

    @Override
    public SourceSplitEnumerator<KuduSourceSplit, KuduSourceState> restoreEnumerator(
           SourceSplitEnumerator.Context<KuduSourceSplit> enumeratorContext, KuduSourceState checkpointState) {
        // todo:
        return new KuduSourceSplitEnumerator(enumeratorContext, partitionParameter);
    }

    @Override
    public Serializer<KuduSourceState> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }

    @Override
    public String getPluginName() {
        return "Kudu";
    }

    @Override
    public void prepare(Config config) {
        String kudumaster = "";
        String tableName = "";
        String columnslist = "";
        CheckResult checkResult = CheckConfigUtil.checkAllExists(config, KuduSourceConfig.KUDU_MASTER.key(), KuduSourceConfig.TABLE_NAME.key(), KuduSourceConfig.COLUMNS_LIST.key());
        if (checkResult.isSuccess()) {
            kudumaster = config.getString(KuduSourceConfig.KUDU_MASTER.key());
            tableName = config.getString(KuduSourceConfig.TABLE_NAME.key());
            columnslist = config.getString(KuduSourceConfig.COLUMNS_LIST.key());
            kuduInputFormat = new KuduInputFormat(kudumaster, tableName, columnslist);
        } else {
            throw new KuduConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, checkResult.getMsg()));
        }
        try {
            KuduClient.KuduClientBuilder kuduClientBuilder = new
                    KuduClient.KuduClientBuilder(kudumaster);
            kuduClientBuilder.defaultOperationTimeoutMs(TIMEOUTMS);

            KuduClient kuduClient = kuduClientBuilder.build();
            partitionParameter = initPartitionParameter(kuduClient, tableName);
            SeaTunnelRowType seaTunnelRowType = getSeaTunnelRowType(kuduClient.openTable(tableName).getSchema().getColumns());
            rowTypeInfo = seaTunnelRowType;
        } catch (KuduException e) {
            throw new KuduConnectorException(KuduConnectorErrorCode.GENERATE_KUDU_PARAMETERS_FAILED, e);
        }
    }

    private PartitionParameter initPartitionParameter(KuduClient kuduClient, String tableName) {
        String keyColumn = null;
        int maxKey = 0;
        int minKey = 0;
        boolean flag = true;
        try {
            KuduScanner.KuduScannerBuilder kuduScannerBuilder =
                    kuduClient.newScannerBuilder(kuduClient.openTable(tableName));
            ArrayList<String> columnsList = new ArrayList<String>();
            keyColumn = kuduClient.openTable(tableName).getSchema().getPrimaryKeyColumns().get(0).getName();
            columnsList.add("" + keyColumn);
            kuduScannerBuilder.setProjectedColumnNames(columnsList);
            KuduScanner kuduScanner = kuduScannerBuilder.build();
            while (kuduScanner.hasMoreRows()) {
                RowResultIterator rowResults = kuduScanner.nextRows();
                while (rowResults.hasNext()) {
                    RowResult row = rowResults.next();
                    int id = row.getInt("" + keyColumn);
                    if (flag) {
                        maxKey = id;
                        minKey = id;
                        flag = false;
                    } else {
                        if (id >= maxKey) {
                            maxKey = id;
                        }
                        if (id <= minKey) {
                            minKey = id;
                        }
                    }
                }
            }
        } catch (KuduException e) {
            throw new KuduConnectorException(KuduConnectorErrorCode.GENERATE_KUDU_PARAMETERS_FAILED, "Failed to generate upper and lower limits for each partition");
        }
        return new PartitionParameter(keyColumn, Long.parseLong(minKey + ""), Long.parseLong(maxKey + ""));
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
            throw new KuduConnectorException(CommonErrorCode.TABLE_SCHEMA_GET_FAILED, String.format("PluginName: %s, PluginType: %s, Message: %s",
                    "Kudu", PluginType.SOURCE, ExceptionUtils.getMessage(e)));
        }
        return new SeaTunnelRowType(fieldNames.toArray(new String[0]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }
}
