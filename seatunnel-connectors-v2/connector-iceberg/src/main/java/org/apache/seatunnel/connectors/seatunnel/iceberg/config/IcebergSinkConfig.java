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

package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.google.common.collect.ImmutableList;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

@Getter
@Setter
@ToString
public class IcebergSinkConfig extends IcebergCommonConfig {

    private static final long serialVersionUID = -2790210008337142246L;

    public static final int SCHEMA_UPDATE_RETRIES = 2; // 3 total attempts
    public static final int CREATE_TABLE_RETRIES = 2; // 3 total attempts

    private static final String ID_COLUMNS = "id-columns";
    private static final String PARTITION_BY = "partition-by";

    @VisibleForTesting private static final String COMMA_NO_PARENS_REGEX = ",(?![^()]*+\\))";

    private final ReadonlyConfig readonlyConfig;
    private Map<String, String> autoCreateProps;
    private Map<String, String> writeProps;
    private List<String> primaryKeys;
    private List<String> partitionKeys;
    private String commitBranch;

    private boolean upsertModeEnabled;
    private boolean tableSchemaEvolutionEnabled;
    private SchemaSaveMode schemaSaveMode;
    private DataSaveMode dataSaveMode;
    private String dataSaveModeSQL;

    public IcebergSinkConfig(ReadonlyConfig readonlyConfig) {
        super(readonlyConfig);
        this.readonlyConfig = readonlyConfig;
        this.autoCreateProps = readonlyConfig.get(IcebergSinkOptions.AUTO_CREATE_PROPS);
        this.writeProps = readonlyConfig.get(IcebergSinkOptions.WRITE_PROPS);
        this.primaryKeys =
                stringToList(readonlyConfig.get(IcebergSinkOptions.TABLE_PRIMARY_KEYS), ",");
        this.partitionKeys =
                stringToList(
                        readonlyConfig.get(IcebergSinkOptions.TABLE_DEFAULT_PARTITION_KEYS), ",");
        this.upsertModeEnabled =
                readonlyConfig.get(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP);
        this.tableSchemaEvolutionEnabled =
                readonlyConfig.get(IcebergSinkOptions.TABLE_SCHEMA_EVOLUTION_ENABLED_PROP);
        this.schemaSaveMode = readonlyConfig.get(IcebergSinkOptions.SCHEMA_SAVE_MODE);
        this.dataSaveMode = readonlyConfig.get(IcebergSinkOptions.DATA_SAVE_MODE);
        this.dataSaveModeSQL = readonlyConfig.get(IcebergSinkOptions.DATA_SAVE_MODE_CUSTOM_SQL);
        this.commitBranch = readonlyConfig.get(IcebergSinkOptions.TABLES_DEFAULT_COMMIT_BRANCH);
    }

    @VisibleForTesting
    public static List<String> stringToList(String value, String regex) {
        if (value == null || value.isEmpty()) {
            return ImmutableList.of();
        }
        return Arrays.stream(value.split(regex)).map(String::trim).collect(toList());
    }
}
