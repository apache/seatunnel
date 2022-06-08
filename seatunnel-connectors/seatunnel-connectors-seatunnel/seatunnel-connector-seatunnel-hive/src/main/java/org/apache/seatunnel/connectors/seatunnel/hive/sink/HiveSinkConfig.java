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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;

import java.util.List;

@Data
public class HiveSinkConfig {

    private static final String HIVE_SAVE_MODE = "save_mode";

    private static final String HIVE_SINK_COLUMNS = "sink_columns";

    private static final String HIVE_PARTITION_BY = "partition_by";

    private static final String HIVE_RESULT_TABLE_NAME = "result_table_name";

    private static final String SINK_TMP_FS_ROOT_PATH = "sink_tmp_fs_root_path";

    private static final String HIVE_TABLE_FS_PATH = "hive_table_fs_path";

    private static final String HIVE_TXT_FILE_FIELD_DELIMITER = "hive_txt_file_field_delimiter";

    private static final String HIVE_TXT_FILE_LINE_DELIMITER = "hive_txt_file_line_delimiter";

    private SaveMode saveMode;

    private String sinkTmpFsRootPath;

    private List<String> partitionFieldNames;

    private String hiveTableName;

    private List<String> sinkColumns;

    private String hiveTableFsPath;

    private String hiveTxtFileFieldDelimiter;

    private String hiveTxtFileLineDelimiter;

    public enum SaveMode {
        APPEND(),
        OVERWRITE();

        public static SaveMode fromStr(String str) {
            if ("overwrite".equals(str)) {
                return OVERWRITE;
            } else {
                return APPEND;
            }
        }
    }

    public HiveSinkConfig(Config pluginConfig) {
        this.saveMode = SaveMode.fromStr(pluginConfig.getString(HIVE_SAVE_MODE));
        this.sinkTmpFsRootPath = pluginConfig.getString(SINK_TMP_FS_ROOT_PATH);
        this.hiveTableName = pluginConfig.getString(HIVE_RESULT_TABLE_NAME);
        this.partitionFieldNames = pluginConfig.getStringList(HIVE_PARTITION_BY);
        this.sinkColumns = pluginConfig.getStringList(HIVE_SINK_COLUMNS);
        this.hiveTxtFileFieldDelimiter = pluginConfig.getString(HIVE_TXT_FILE_FIELD_DELIMITER);
        this.hiveTxtFileLineDelimiter = pluginConfig.getString(HIVE_TXT_FILE_FIELD_DELIMITER);
    }
}
