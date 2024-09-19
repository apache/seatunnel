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

package org.apache.seatunnel.connectors.seatunnel.hudi.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

@Data
@Builder(builderClassName = "Builder")
public class HudiSinkConfig implements Serializable {

    private static final long serialVersionUID = 2L;

    private String tableDfsPath;

    private List<HudiTableConfig> tableList;

    private String confFilesPath;

    private boolean autoCommit;

    private SchemaSaveMode schemaSaveMode;

    private DataSaveMode dataSaveMode;

    public static HudiSinkConfig of(ReadonlyConfig config) {
        Builder builder = HudiSinkConfig.builder();
        Optional<Boolean> optionalAutoCommit = config.getOptional(HudiOptions.AUTO_COMMIT);
        Optional<SchemaSaveMode> optionalSchemaSaveMode =
                config.getOptional(HudiOptions.SCHEMA_SAVE_MODE);
        Optional<DataSaveMode> optionalDataSaveMode =
                config.getOptional(HudiOptions.DATA_SAVE_MODE);

        builder.tableDfsPath(config.get(HudiOptions.TABLE_DFS_PATH));
        builder.confFilesPath(config.get(HudiOptions.CONF_FILES_PATH));
        builder.tableList(HudiTableConfig.of(config));

        builder.autoCommit(optionalAutoCommit.orElseGet(HudiOptions.AUTO_COMMIT::defaultValue));
        builder.schemaSaveMode(
                optionalSchemaSaveMode.orElseGet(HudiOptions.SCHEMA_SAVE_MODE::defaultValue));
        builder.dataSaveMode(
                optionalDataSaveMode.orElseGet(HudiOptions.DATA_SAVE_MODE::defaultValue));
        return builder.build();
    }
}
