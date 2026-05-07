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

package org.apache.seatunnel.connectors.seatunnel.bigtable.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Builder
@Getter
public class BigtableParameters implements Serializable {

    private String projectId;
    private String instanceId;
    private String table;
    private List<String> rowkeyColumns;
    private String credentialsPath;

    // Sink-specific
    private Map<String, String> columnFamily;
    @Builder.Default private String rowkeyDelimiter = "";
    private String versionColumn;

    @Builder.Default
    private BigtableSinkOptions.NullMode nullMode = BigtableSinkOptions.NullMode.SKIP;

    @Builder.Default private int batchMutationSize = 100;

    // Source-specific
    private String startRowkey;
    private String endRowkey;
    private Long startTimestamp;
    private Long endTimestamp;
    @Builder.Default private int maxVersions = 1;
    @Builder.Default private int scanRowLimit = -1;

    public static BigtableParameters buildWithConfig(ReadonlyConfig config) {
        BigtableParametersBuilder builder = BigtableParameters.builder();
        builder.projectId(config.get(BigtableBaseOptions.PROJECT_ID));
        builder.instanceId(config.get(BigtableBaseOptions.INSTANCE_ID));
        builder.table(config.get(BigtableBaseOptions.TABLE));
        builder.rowkeyColumns(config.get(BigtableBaseOptions.ROWKEY_COLUMNS));

        config.getOptional(BigtableBaseOptions.CREDENTIALS_PATH)
                .ifPresent(builder::credentialsPath);
        config.getOptional(BigtableSinkOptions.COLUMN_FAMILY).ifPresent(builder::columnFamily);
        config.getOptional(BigtableSinkOptions.ROWKEY_DELIMITER)
                .ifPresent(builder::rowkeyDelimiter);
        config.getOptional(BigtableSinkOptions.VERSION_COLUMN).ifPresent(builder::versionColumn);
        config.getOptional(BigtableSinkOptions.NULL_MODE).ifPresent(builder::nullMode);
        config.getOptional(BigtableSinkOptions.BATCH_MUTATION_SIZE)
                .ifPresent(builder::batchMutationSize);
        return builder.build();
    }

    public static BigtableParameters buildWithSourceConfig(ReadonlyConfig config) {
        BigtableParametersBuilder builder = BigtableParameters.builder();
        builder.projectId(config.get(BigtableBaseOptions.PROJECT_ID));
        builder.instanceId(config.get(BigtableBaseOptions.INSTANCE_ID));
        builder.table(config.get(BigtableBaseOptions.TABLE));

        config.getOptional(BigtableBaseOptions.ROWKEY_COLUMNS).ifPresent(builder::rowkeyColumns);
        config.getOptional(BigtableBaseOptions.CREDENTIALS_PATH)
                .ifPresent(builder::credentialsPath);
        config.getOptional(BigtableSourceOptions.START_ROW_KEY).ifPresent(builder::startRowkey);
        config.getOptional(BigtableSourceOptions.END_ROW_KEY).ifPresent(builder::endRowkey);
        config.getOptional(BigtableSourceOptions.START_TIMESTAMP)
                .ifPresent(builder::startTimestamp);
        config.getOptional(BigtableSourceOptions.END_TIMESTAMP).ifPresent(builder::endTimestamp);
        config.getOptional(BigtableSourceOptions.MAX_VERSIONS).ifPresent(builder::maxVersions);
        config.getOptional(BigtableSourceOptions.SCAN_ROW_LIMIT).ifPresent(builder::scanRowLimit);
        return builder.build();
    }
}
