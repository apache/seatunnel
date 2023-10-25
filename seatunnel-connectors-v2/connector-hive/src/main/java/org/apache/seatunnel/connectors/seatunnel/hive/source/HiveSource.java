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

package org.apache.seatunnel.connectors.seatunnel.hive.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.BaseHdfsFileSource;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.auto.service.AutoService;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig.FILE_FORMAT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig.FILE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig.ORC_INPUT_FORMAT_CLASSNAME;
import static org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig.PARQUET_INPUT_FORMAT_CLASSNAME;
import static org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig.TEXT_INPUT_FORMAT_CLASSNAME;

@AutoService(SeaTunnelSource.class)
public class HiveSource extends BaseHdfsFileSource {
    private Table tableInformation;

    @Override
    public String getPluginName() {
        return "Hive";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig, HiveConfig.METASTORE_URI.key(), HiveConfig.TABLE_NAME.key());
        if (!result.isSuccess()) {
            throw new HiveConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        result =
                CheckConfigUtil.checkAtLeastOneExists(
                        pluginConfig,
                        TableSchemaOptions.SCHEMA.key(),
                        FILE_FORMAT_TYPE.key(),
                        FILE_PATH.key(),
                        FS_DEFAULT_NAME_KEY);
        if (result.isSuccess()) {
            throw new HiveConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Hive source connector does not support these setting [%s]",
                            String.join(
                                    ",",
                                    TableSchemaOptions.SCHEMA.key(),
                                    FILE_FORMAT_TYPE.key(),
                                    FILE_PATH.key(),
                                    FS_DEFAULT_NAME_KEY)));
        }
        if (pluginConfig.hasPath(BaseSourceConfig.READ_PARTITIONS.key())) {
            // verify partition list
            List<String> partitionsList =
                    pluginConfig.getStringList(BaseSourceConfig.READ_PARTITIONS.key());
            if (partitionsList.isEmpty()) {
                throw new HiveConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "Partitions list is empty, please check");
            }
            int depth = partitionsList.get(0).replaceAll("\\\\", "/").split("/").length;
            long count =
                    partitionsList.stream()
                            .map(partition -> partition.replaceAll("\\\\", "/").split("/").length)
                            .filter(length -> length != depth)
                            .count();
            if (count > 0) {
                throw new HiveConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "Every partition that in partition list should has the same directory depth");
            }
        }
        Pair<String[], Table> tableInfo = HiveConfig.getTableInfo(pluginConfig);
        tableInformation = tableInfo.getRight();
        String inputFormat = tableInformation.getSd().getInputFormat();
        if (TEXT_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            pluginConfig =
                    pluginConfig.withValue(
                            FILE_FORMAT_TYPE.key(),
                            ConfigValueFactory.fromAnyRef(FileFormat.TEXT.toString()));
            // Build schema from hive table information
            // Because the entrySet in typesafe config couldn't keep key-value order
            // So use jackson to keep key-value order
            Map<String, Object> schema = parseSchema(tableInformation);
            ConfigRenderOptions options = ConfigRenderOptions.concise();
            String render = pluginConfig.root().render(options);
            ObjectNode jsonNodes = JsonUtils.parseObject(render);
            jsonNodes.putPOJO(TableSchemaOptions.SCHEMA.key(), schema);
            pluginConfig = ConfigFactory.parseString(jsonNodes.toString());
        } else if (PARQUET_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            pluginConfig =
                    pluginConfig.withValue(
                            FILE_FORMAT_TYPE.key(),
                            ConfigValueFactory.fromAnyRef(FileFormat.PARQUET.toString()));
        } else if (ORC_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            pluginConfig =
                    pluginConfig.withValue(
                            FILE_FORMAT_TYPE.key(),
                            ConfigValueFactory.fromAnyRef(FileFormat.ORC.toString()));
        } else {
            throw new HiveConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    "Hive connector only support [text parquet orc] table now");
        }
        String hdfsLocation = tableInformation.getSd().getLocation();
        try {
            URI uri = new URI(hdfsLocation);
            String path = uri.getPath();
            String defaultFs = hdfsLocation.replace(path, "");
            pluginConfig =
                    pluginConfig
                            .withValue(
                                    BaseSourceConfig.FILE_PATH.key(),
                                    ConfigValueFactory.fromAnyRef(path))
                            .withValue(
                                    FS_DEFAULT_NAME_KEY, ConfigValueFactory.fromAnyRef(defaultFs));
        } catch (URISyntaxException e) {
            String errorMsg =
                    String.format(
                            "Get hdfs namenode host from table location [%s] failed,"
                                    + "please check it",
                            hdfsLocation);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HDFS_NAMENODE_HOST_FAILED, errorMsg, e);
        }
        super.prepare(pluginConfig);
    }

    private Map<String, Object> parseSchema(Table table) {
        LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
        LinkedHashMap<String, Object> schema = new LinkedHashMap<>();
        List<FieldSchema> cols = table.getSd().getCols();
        for (FieldSchema col : cols) {
            String name = col.getName();
            String type = col.getType();
            fields.put(name, covertHiveTypeToSeaTunnelType(type));
        }
        schema.put("fields", fields);
        return schema;
    }

    private Object covertHiveTypeToSeaTunnelType(String hiveType) {
        if (hiveType.contains("varchar")) {
            return SqlType.STRING;
        }
        if (hiveType.contains("char")) {
            throw new HiveConnectorException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "SeaTunnel hive connector does not supported char type in text table");
        }
        if (hiveType.contains("binary")) {
            return SqlType.BYTES.name();
        }
        if (hiveType.contains("struct")) {
            LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
            int start = hiveType.indexOf("<");
            int end = hiveType.lastIndexOf(">");
            String[] columns = hiveType.substring(start + 1, end).split(",");
            for (String column : columns) {
                String[] splits = column.split(":");
                fields.put(splits[0], covertHiveTypeToSeaTunnelType(splits[1]));
            }
            return fields;
        }
        return hiveType;
    }
}
