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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import static org.apache.seatunnel.connectors.seatunnel.file.config.Constant.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.file.config.Constant.FILE_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.file.config.Constant.FILE_NAME_EXPRESSION;
import static org.apache.seatunnel.connectors.seatunnel.file.config.Constant.IS_PARTITION_FIELD_WRITE_IN_FILE;
import static org.apache.seatunnel.connectors.seatunnel.file.config.Constant.PATH;
import static org.apache.seatunnel.connectors.seatunnel.file.config.Constant.ROW_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.file.config.Constant.SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig.ORC_OUTPUT_FORMAT_CLASSNAME;
import static org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig.PARQUET_OUTPUT_FORMAT_CLASSNAME;
import static org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig.TEXT_OUTPUT_FORMAT_CLASSNAME;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.BaseFileSink;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.SaveMode;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import com.google.auto.service.AutoService;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AutoService(SeaTunnelSink.class)
public class HiveSink extends BaseFileSink {
    private String dbName;
    private String tableName;
    private Table tableInformation;

    @Override
    public String getPluginName() {
        return "Hive";
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        super.setTypeInfo(seaTunnelRowType);
        HiveMetaStoreProxy hiveMetaStoreProxy = HiveMetaStoreProxy.getInstance(pluginConfig);
        // --------------------Check textFileSinkConfig with the hive table info-------------------
        List<FieldSchema> fields = hiveMetaStoreProxy.getTableFields(dbName, tableName);
        List<FieldSchema> partitionKeys = tableInformation.getPartitionKeys();

        // Remove partitionKeys from table fields
        List<FieldSchema> fieldNotContainPartitionKey = fields.stream().filter(filed -> !partitionKeys.contains(filed)).collect(Collectors.toList());

        // check fields size must same as sinkColumnList size
        if (fieldNotContainPartitionKey.size() != textFileSinkConfig.getSinkColumnList().size()) {
            throw new RuntimeException("sink columns size must same as hive table field size");
        }

        // check hivePartitionFieldList size must same as partitionFieldList size
        if (partitionKeys.size() != textFileSinkConfig.getPartitionFieldList().size()) {
            throw new RuntimeException("partition by columns size must same as hive table partition columns size");
        }
        hiveMetaStoreProxy.close();
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, HiveConfig.METASTORE_URI, HiveConfig.TABLE_NAME);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SINK, result.getMsg());
        }
        Pair<String[], Table> tableInfo = HiveConfig.getTableInfo(pluginConfig);
        dbName = tableInfo.getLeft()[0];
        tableName = tableInfo.getLeft()[1];
        tableInformation = tableInfo.getRight();
        String outputFormat = tableInformation.getSd().getOutputFormat();
        if (TEXT_OUTPUT_FORMAT_CLASSNAME.equals(outputFormat)) {
            Map<String, String> parameters = tableInformation.getSd().getSerdeInfo().getParameters();
            pluginConfig = pluginConfig.withValue(FILE_FORMAT, ConfigValueFactory.fromAnyRef(FileFormat.TEXT.toString()))
                    .withValue(FIELD_DELIMITER, ConfigValueFactory.fromAnyRef(parameters.get("field.delim")))
                    .withValue(ROW_DELIMITER, ConfigValueFactory.fromAnyRef(parameters.get("line.delim")));
        } else if (PARQUET_OUTPUT_FORMAT_CLASSNAME.equals(outputFormat)) {
            pluginConfig = pluginConfig.withValue(FILE_FORMAT, ConfigValueFactory.fromAnyRef(FileFormat.PARQUET.toString()));
        } else if (ORC_OUTPUT_FORMAT_CLASSNAME.equals(outputFormat)) {
            pluginConfig = pluginConfig.withValue(FILE_FORMAT, ConfigValueFactory.fromAnyRef(FileFormat.ORC.toString()));
        } else {
            throw new RuntimeException("Only support [text parquet orc] file now");
        }
        pluginConfig = pluginConfig.withValue(IS_PARTITION_FIELD_WRITE_IN_FILE, ConfigValueFactory.fromAnyRef(false))
                .withValue(FILE_NAME_EXPRESSION, ConfigValueFactory.fromAnyRef("${transactionId}"))
                .withValue(PATH, ConfigValueFactory.fromAnyRef(tableInformation.getSd().getLocation()));

        if (!pluginConfig.hasPath(SAVE_MODE) || StringUtils.isBlank(pluginConfig.getString(SAVE_MODE))) {
            pluginConfig = pluginConfig.withValue(SAVE_MODE, ConfigValueFactory.fromAnyRef(SaveMode.APPEND.toString()));
        }
        String hdfsLocation = tableInformation.getSd().getLocation();
        try {
            URI uri = new URI(hdfsLocation);
            String path = uri.getPath();
            pluginConfig = pluginConfig.withValue(PATH, ConfigValueFactory.fromAnyRef(path));
            // hadoopConf = new HadoopConf(hdfsLocation.replace(path, ""));
            hadoopConf = new HadoopConf("hdfs://namenode14812");
        } catch (URISyntaxException e) {
            throw new RuntimeException("Get hdfs cluster address failed, please check.", e);
        }
        super.prepare(pluginConfig);
    }
}
