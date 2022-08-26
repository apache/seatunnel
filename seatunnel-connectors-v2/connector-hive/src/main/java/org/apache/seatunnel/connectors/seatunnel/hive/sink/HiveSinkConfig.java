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
import static org.apache.seatunnel.connectors.seatunnel.hive.config.Constant.HIVE_METASTORE_URIS;
import static org.apache.seatunnel.connectors.seatunnel.hive.config.Constant.HIVE_RESULT_TABLE_NAME;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.Constant;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.SaveMode;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.TextFileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import lombok.Data;
import lombok.NonNull;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class HiveSinkConfig implements Serializable {
    private static final String TEXT_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    private static final String PARQUET_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    private static final String ORC_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
    private String hiveTableName;
    private List<String> hivePartitionFieldList;
    private String hiveMetaUris;

    private String dbName;

    private String tableName;

    private Table table;

    private TextFileSinkConfig textFileSinkConfig;

    public HiveSinkConfig(@NonNull Config config, @NonNull SeaTunnelRowType seaTunnelRowTypeInfo) {
        checkArgument(!CollectionUtils.isEmpty(Arrays.asList(seaTunnelRowTypeInfo.getFieldNames())));

        if (config.hasPath(HIVE_RESULT_TABLE_NAME) && !StringUtils.isBlank(config.getString(HIVE_RESULT_TABLE_NAME))) {
            this.hiveTableName = config.getString(HIVE_RESULT_TABLE_NAME);
        }
        checkNotNull(hiveTableName);

        if (config.hasPath(HIVE_METASTORE_URIS) && !StringUtils.isBlank(config.getString(HIVE_METASTORE_URIS))) {
            this.hiveMetaUris = config.getString(HIVE_METASTORE_URIS);
        }
        checkNotNull(hiveMetaUris);

        String[] dbAndTableName = hiveTableName.split("\\.");
        if (dbAndTableName == null || dbAndTableName.length != 2) {
            throw new RuntimeException("Please config " + HIVE_RESULT_TABLE_NAME + " as db.table format");
        }
        this.dbName = dbAndTableName[0];
        this.tableName = dbAndTableName[1];
        HiveMetaStoreProxy hiveMetaStoreProxy = new HiveMetaStoreProxy(hiveMetaUris);
        HiveMetaStoreClient hiveMetaStoreClient = hiveMetaStoreProxy.getHiveMetaStoreClient();

        try {
            table = hiveMetaStoreClient.getTable(dbName, tableName);
            String outputFormat = table.getSd().getOutputFormat();
            Map<String, String> parameters = table.getSd().getSerdeInfo().getParameters();
            if (TEXT_FORMAT_CLASSNAME.equals(outputFormat)) {
                config = config.withValue(FILE_FORMAT, ConfigValueFactory.fromAnyRef(FileFormat.TEXT.toString()))
                        .withValue(FIELD_DELIMITER, ConfigValueFactory.fromAnyRef(parameters.get("field.delim")))
                        .withValue(ROW_DELIMITER, ConfigValueFactory.fromAnyRef(parameters.get("line.delim")));
            } else if (PARQUET_FORMAT_CLASSNAME.equals(outputFormat)) {
                config = config.withValue(FILE_FORMAT, ConfigValueFactory.fromAnyRef(FileFormat.PARQUET.toString()));
            } else if (ORC_FORMAT_CLASSNAME.equals(outputFormat)) {
                config = config.withValue(FILE_FORMAT, ConfigValueFactory.fromAnyRef(FileFormat.ORC.toString()));
            } else {
                throw new RuntimeException("Only support [text parquet orc] file now");
            }

            config = config.withValue(IS_PARTITION_FIELD_WRITE_IN_FILE, ConfigValueFactory.fromAnyRef(false))
                .withValue(FILE_NAME_EXPRESSION, ConfigValueFactory.fromAnyRef("${transactionId}"))
                .withValue(PATH, ConfigValueFactory.fromAnyRef(table.getSd().getLocation()));

            if (!config.hasPath(SAVE_MODE) || StringUtils.isBlank(config.getString(Constant.SAVE_MODE))) {
                config = config.withValue(SAVE_MODE, ConfigValueFactory.fromAnyRef(SaveMode.APPEND.toString()));
            }

            this.textFileSinkConfig = new TextFileSinkConfig(config, seaTunnelRowTypeInfo);

            // --------------------Check textFileSinkConfig with the hive table info-------------------
            List<FieldSchema> fields = hiveMetaStoreClient.getFields(dbAndTableName[0], dbAndTableName[1]);
            List<FieldSchema> partitionKeys = table.getPartitionKeys();

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

            // --------------------Check textFileSinkConfig with the hive table info end----------------
        } catch (TException e) {
            throw new RuntimeException(e);
        } finally {
            hiveMetaStoreClient.close();
        }

        // hive only support append or overwrite
        if (!this.textFileSinkConfig.getSaveMode().equals(SaveMode.APPEND) && !this.textFileSinkConfig.getSaveMode().equals(SaveMode.OVERWRITE)) {
            throw new RuntimeException("hive only support append or overwrite save mode");
        }
    }

    public TextFileSinkConfig getTextFileSinkConfig() {
        return textFileSinkConfig;
    }
}
