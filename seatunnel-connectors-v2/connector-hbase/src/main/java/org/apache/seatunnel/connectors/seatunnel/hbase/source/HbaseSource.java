/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hbase.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.COLUMNS;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ZOOKEEPER_QUORUM;

@AutoService(SeaTunnelSource.class)
public class HbaseSource
        implements SeaTunnelSource<SeaTunnelRow, HbaseSourceSplit, HbaseSourceState> {
    private static final Logger LOG = LoggerFactory.getLogger(HbaseSource.class);

    public static final String PLUGIN_NAME = "Hbase";
    private Config pluginConfig;
    private String tableName;
    private SeaTunnelRowType seaTunnelRowType;

    private HbaseParameters hbaseParameters;

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig, ZOOKEEPER_QUORUM.key(), TABLE.key(), COLUMNS.key());
        if (!result.isSuccess()) {
            throw new HbaseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        this.hbaseParameters = HbaseParameters.buildWithSinkConfig(pluginConfig);
        SeaTunnelRowType typeInfo;
        typeInfo = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
        this.seaTunnelRowType = typeInfo;

        //        String[] fieldNames = hbaseParameters.getColumns().toArray(new String[0]);
        //        SeaTunnelDataType<?>[] types = new SeaTunnelDataType<?>[fieldNames.length];
        //        Arrays.fill(types, STRING_TYPE);
        //        SeaTunnelDataType<?>[] types = {
        //            STRING_TYPE, STRING_TYPE, STRING_TYPE, STRING_TYPE, STRING_TYPE
        //        };
        // this.seaTunnelRowType = new SeaTunnelRowType(fieldNames, types);

        //        Configuration hbaseConfiguration = HBaseConfiguration.create();
        //        hbaseConfiguration.set("hbase.zookeeper.quorum",
        // hbaseParameters.getZookeeperQuorum());
        //        if (hbaseParameters.getHbaseExtraConfig() != null) {
        //            hbaseParameters.getHbaseExtraConfig().forEach(hbaseConfiguration::set);
        //        }
        //
        //        Connection connection = null;
        //        // initialize hbase connection
        //        try {
        //            connection = ConnectionFactory.createConnection(hbaseConfiguration);
        //            TableName tableName = TableName.valueOf("test_table");
        //
        //            // 获取Admin实例
        //            try (Admin admin = connection.getAdmin()) {
        //                // 检查表是否存在
        //                if (admin.tableExists(tableName)) {
        //                    // 获取表的列描述符
        //                    HTableDescriptor tableDescriptor =
        // admin.getTableDescriptor(tableName);
        //
        //                    // 获取所有列族
        //                    HColumnDescriptor[] columnDescriptors =
        // tableDescriptor.getColumnFamilies();
        //
        //                    // 输出列族信息
        //                    for (HColumnDescriptor columnDescriptor : columnDescriptors) {
        //                        System.out.println("Column Family: " +
        // columnDescriptor.getNameAsString());
        //                    }
        //                }
        //                // this.seaTunnelRowType =
        // CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
        //            } catch (IOException e) {
        //                throw new RuntimeException(e);
        //            }
        //        } catch (IOException e) {
        //            throw new RuntimeException(e);
        //        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }

    @Override
    public SourceReader<SeaTunnelRow, HbaseSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new HbaseSourceReader(hbaseParameters, readerContext, seaTunnelRowType);
    }

    @Override
    public SourceSplitEnumerator<HbaseSourceSplit, HbaseSourceState> createEnumerator(
            SourceSplitEnumerator.Context<HbaseSourceSplit> enumeratorContext) throws Exception {
        return new HbaseSourceSplitEnumerator(enumeratorContext, hbaseParameters);
    }

    @Override
    public SourceSplitEnumerator<HbaseSourceSplit, HbaseSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<HbaseSourceSplit> enumeratorContext,
            HbaseSourceState checkpointState)
            throws Exception {
        return new HbaseSourceSplitEnumerator(enumeratorContext, hbaseParameters, checkpointState);
    }
}
