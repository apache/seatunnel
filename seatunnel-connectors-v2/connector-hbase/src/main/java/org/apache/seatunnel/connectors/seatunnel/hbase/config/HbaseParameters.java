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

package org.apache.seatunnel.connectors.seatunnel.hbase.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Builder
@Getter
public class HbaseParameters implements Serializable {

    private String zookeeperQuorum;

    private String namespace;

    private String table;

    private List<String> rowkeyColumns;

    private List<String> columns;

    private Map<String, String> familyNames;

    private String versionColumn;

    private Map<String, String> hbaseExtraConfig;

    @Builder.Default private int caching = HbaseSinkOptions.HBASE_CACHING_CONFIG.defaultValue();

    @Builder.Default private int batch = HbaseSinkOptions.HBASE_BATCH_CONFIG.defaultValue();

    @Builder.Default private Long ttl = HbaseSinkOptions.HBASE_TTL_CONFIG.defaultValue();

    @Builder.Default
    private boolean cacheBlocks = HbaseSinkOptions.HBASE_CACHE_BLOCKS_CONFIG.defaultValue();

    @Builder.Default
    private String rowkeyDelimiter = HbaseSinkOptions.ROWKEY_DELIMITER.defaultValue();

    @Builder.Default
    private HbaseSinkOptions.NullMode nullMode = HbaseSinkOptions.NULL_MODE.defaultValue();

    @Builder.Default private boolean walWrite = HbaseSinkOptions.WAL_WRITE.defaultValue();

    @Builder.Default
    private int writeBufferSize = HbaseSinkOptions.WRITE_BUFFER_SIZE.defaultValue();

    @Builder.Default
    private HbaseSinkOptions.EnCoding enCoding = HbaseSinkOptions.ENCODING.defaultValue();

    public static HbaseParameters buildWithConfig(ReadonlyConfig config) {
        HbaseParametersBuilder builder = HbaseParameters.builder();
        String table = config.get(HbaseBaseOptions.TABLE);
        int colonIndex = table.indexOf(':');
        if (colonIndex != -1) {
            String namespace = table.substring(0, colonIndex);
            builder.namespace(namespace);
            builder.table(table.substring(colonIndex + 1));
        } else {
            builder.table(table);
            builder.namespace("default");
        }

        // required parameters
        builder.zookeeperQuorum(config.get(HbaseBaseOptions.ZOOKEEPER_QUORUM));
        builder.rowkeyColumns(config.get(HbaseBaseOptions.ROWKEY_COLUMNS));
        builder.familyNames(config.get(HbaseSinkOptions.FAMILY_NAME));

        builder.rowkeyDelimiter(config.get(HbaseSinkOptions.ROWKEY_DELIMITER));
        builder.versionColumn(config.get(HbaseSinkOptions.VERSION_COLUMN));
        String nullMode = String.valueOf(config.get(HbaseSinkOptions.NULL_MODE));
        builder.nullMode(HbaseSinkOptions.NullMode.valueOf(nullMode.toUpperCase()));
        builder.walWrite(config.get(HbaseSinkOptions.WAL_WRITE));
        builder.writeBufferSize(config.get(HbaseSinkOptions.WRITE_BUFFER_SIZE));
        String encoding = String.valueOf(config.get(HbaseSinkOptions.ENCODING));
        builder.enCoding(HbaseSinkOptions.EnCoding.valueOf(encoding.toUpperCase()));
        builder.hbaseExtraConfig(config.get(HbaseSinkOptions.HBASE_EXTRA_CONFIG));
        builder.ttl(config.get(HbaseSinkOptions.HBASE_TTL_CONFIG));
        return builder.build();
    }

    public static HbaseParameters buildWithSourceConfig(ReadonlyConfig pluginConfig) {
        HbaseParametersBuilder builder = HbaseParameters.builder();

        // required parameters
        builder.zookeeperQuorum(pluginConfig.get(HbaseBaseOptions.ZOOKEEPER_QUORUM));
        String table = pluginConfig.get(HbaseBaseOptions.TABLE);
        int colonIndex = table.indexOf(':');
        if (colonIndex != -1) {
            String namespace = table.substring(0, colonIndex);
            builder.namespace(namespace);
            builder.table(table.substring(colonIndex + 1));
        } else {
            builder.table(table);
        }

        if (pluginConfig.getOptional(HbaseSinkOptions.HBASE_EXTRA_CONFIG).isPresent()) {
            builder.hbaseExtraConfig(pluginConfig.get(HbaseSinkOptions.HBASE_EXTRA_CONFIG));
        }
        if (pluginConfig.getOptional(HbaseSinkOptions.HBASE_CACHING_CONFIG).isPresent()) {
            builder.caching(pluginConfig.get(HbaseSinkOptions.HBASE_CACHING_CONFIG));
        }
        if (pluginConfig.getOptional(HbaseSinkOptions.HBASE_BATCH_CONFIG).isPresent()) {
            builder.batch(pluginConfig.get(HbaseSinkOptions.HBASE_BATCH_CONFIG));
        }
        if (pluginConfig.getOptional(HbaseSinkOptions.HBASE_CACHE_BLOCKS_CONFIG).isPresent()) {
            builder.cacheBlocks(pluginConfig.get(HbaseSinkOptions.HBASE_CACHE_BLOCKS_CONFIG));
        }
        return builder.build();
    }
}
