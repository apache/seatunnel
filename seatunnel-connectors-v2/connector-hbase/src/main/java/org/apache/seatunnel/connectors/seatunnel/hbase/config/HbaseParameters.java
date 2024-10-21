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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ENCODING;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.FAMILY_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.HBASE_BATCH_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.HBASE_CACHE_BLOCKS_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.HBASE_CACHING_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.HBASE_EXTRA_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.HBASE_TTL_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.NULL_MODE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ROWKEY_COLUMNS;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ROWKEY_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.VERSION_COLUMN;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.WAL_WRITE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.WRITE_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ZOOKEEPER_QUORUM;

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

    @Builder.Default private int caching = HBASE_CACHING_CONFIG.defaultValue();

    @Builder.Default private int batch = HBASE_BATCH_CONFIG.defaultValue();

    @Builder.Default private Long ttl = HBASE_TTL_CONFIG.defaultValue();

    @Builder.Default private boolean cacheBlocks = HBASE_CACHE_BLOCKS_CONFIG.defaultValue();

    @Builder.Default private String rowkeyDelimiter = ROWKEY_DELIMITER.defaultValue();

    @Builder.Default private HbaseConfig.NullMode nullMode = NULL_MODE.defaultValue();

    @Builder.Default private boolean walWrite = WAL_WRITE.defaultValue();

    @Builder.Default private int writeBufferSize = WRITE_BUFFER_SIZE.defaultValue();

    @Builder.Default private HbaseConfig.EnCoding enCoding = ENCODING.defaultValue();

    public static HbaseParameters buildWithConfig(ReadonlyConfig config) {
        HbaseParametersBuilder builder = HbaseParameters.builder();
        String table = config.get(TABLE);
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
        builder.zookeeperQuorum(config.get(ZOOKEEPER_QUORUM));
        builder.rowkeyColumns(config.get(ROWKEY_COLUMNS));
        builder.familyNames(config.get(FAMILY_NAME));

        builder.rowkeyDelimiter(config.get(ROWKEY_DELIMITER));
        builder.versionColumn(config.get(VERSION_COLUMN));
        String nullMode = String.valueOf(config.get(NULL_MODE));
        builder.nullMode(HbaseConfig.NullMode.valueOf(nullMode.toUpperCase()));
        builder.walWrite(config.get(WAL_WRITE));
        builder.writeBufferSize(config.get(WRITE_BUFFER_SIZE));
        String encoding = String.valueOf(config.get(ENCODING));
        builder.enCoding(HbaseConfig.EnCoding.valueOf(encoding.toUpperCase()));
        builder.hbaseExtraConfig(config.get(HBASE_EXTRA_CONFIG));
        builder.ttl(config.get(HBASE_TTL_CONFIG));
        return builder.build();
    }

    public static HbaseParameters buildWithSourceConfig(Config pluginConfig) {
        HbaseParametersBuilder builder = HbaseParameters.builder();

        // required parameters
        builder.zookeeperQuorum(pluginConfig.getString(ZOOKEEPER_QUORUM.key()));
        String table = pluginConfig.getString(TABLE.key());
        int colonIndex = table.indexOf(':');
        if (colonIndex != -1) {
            String namespace = table.substring(0, colonIndex);
            builder.namespace(namespace);
            builder.table(table.substring(colonIndex + 1));
        } else {
            builder.table(table);
        }

        if (pluginConfig.hasPath(HBASE_EXTRA_CONFIG.key())) {
            Config extraConfig = pluginConfig.getConfig(HBASE_EXTRA_CONFIG.key());
            builder.hbaseExtraConfig(TypesafeConfigUtils.configToMap(extraConfig));
        }
        if (pluginConfig.hasPath(HBASE_CACHING_CONFIG.key())) {
            builder.caching(pluginConfig.getInt(HBASE_CACHING_CONFIG.key()));
        }
        if (pluginConfig.hasPath(HBASE_BATCH_CONFIG.key())) {
            builder.batch(pluginConfig.getInt(HBASE_BATCH_CONFIG.key()));
        }
        if (pluginConfig.hasPath(HBASE_CACHE_BLOCKS_CONFIG.key())) {
            builder.cacheBlocks(pluginConfig.getBoolean(HBASE_CACHE_BLOCKS_CONFIG.key()));
        }
        return builder.build();
    }
}
