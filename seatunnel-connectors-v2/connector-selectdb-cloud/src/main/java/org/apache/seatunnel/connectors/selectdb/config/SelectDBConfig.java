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

package org.apache.seatunnel.connectors.selectdb.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.common.config.CheckConfigUtil;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Setter
@Getter
@ToString
public class SelectDBConfig {
    private static final int DEFAULT_SINK_MAX_RETRIES = 3;
    private static final int DEFAULT_SINK_BUFFER_SIZE = 10 * 1024 * 1024;
    private static final int DEFAULT_SINK_BUFFER_COUNT = 10000;
    // common option
    public static final Option<String> LOAD_URL =
            Options.key("load-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SelectDB load http address.");
    public static final Option<String> JDBC_URL =
            Options.key("jdbc-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SelectDB jdbc query address.");
    public static final Option<String> CLUSTER_NAME =
            Options.key("cluster-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SelectDB cluster name.");

    public static final Option<String> TABLE_IDENTIFIER =
            Options.key("table.identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc table name.");
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc user name.");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc password.");

    public static final Option<Boolean> SINK_ENABLE_2PC =
            Options.key("sink.enable-2pc")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("enable 2PC while loading");
    // sink config options
    public static final Option<Integer> SINK_MAX_RETRIES =
            Options.key("sink.max-retries")
                    .intType()
                    .defaultValue(DEFAULT_SINK_MAX_RETRIES)
                    .withDescription("the max retry times if writing records to database failed.");
    public static final Option<Integer> SINK_BUFFER_SIZE =
            Options.key("sink.buffer-size")
                    .intType()
                    .defaultValue(DEFAULT_SINK_BUFFER_SIZE)
                    .withDescription("the buffer size to cache data for stream load.");
    public static final Option<Integer> SINK_BUFFER_COUNT =
            Options.key("sink.buffer-count")
                    .intType()
                    .defaultValue(DEFAULT_SINK_BUFFER_COUNT)
                    .withDescription("the buffer count to cache data for stream load.");
    public static final Option<String> SINK_LABEL_PREFIX =
            Options.key("sink.label-prefix")
                    .stringType()
                    .defaultValue(UUID.randomUUID().toString())
                    .withDescription("the unique label prefix.");
    public static final Option<Boolean> SINK_ENABLE_DELETE =
            Options.key("sink.enable-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to enable the delete function");

    public static final Option<Integer> SINK_FLUSH_QUEUE_SIZE =
            Options.key("sink.flush.queue-size")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Queue length for async upload to object storage");

    public static final Option<Map<String, String>> SELECTDB_SINK_CONFIG_PREFIX =
            Options.key("selectdb.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The parameter of the Copy Into data_desc. "
                                    + "The way to specify the parameter is to add the prefix `selectdb.config` to the original load parameter name ");

    private String loadUrl;
    private String jdbcUrl;
    private String clusterName;
    private String username;
    private String password;
    private String tableIdentifier;
    private Boolean enableDelete;
    private String labelPrefix;
    private boolean enable2PC;
    private Integer maxRetries;
    private Integer bufferSize;
    private Integer bufferCount;
    private Integer flushQueueSize;
    private Properties StageLoadProps;

    public static SelectDBConfig loadConfig(Config pluginConfig) {
        SelectDBConfig selectdbConfig = new SelectDBConfig();
        selectdbConfig.setLoadUrl(pluginConfig.getString(LOAD_URL.key()));
        selectdbConfig.setJdbcUrl(pluginConfig.getString(JDBC_URL.key()));
        selectdbConfig.setClusterName(pluginConfig.getString(CLUSTER_NAME.key()));
        selectdbConfig.setUsername(pluginConfig.getString(USERNAME.key()));
        selectdbConfig.setPassword(pluginConfig.getString(PASSWORD.key()));
        selectdbConfig.setTableIdentifier(pluginConfig.getString(TABLE_IDENTIFIER.key()));
        selectdbConfig.setStageLoadProps(parseCopyIntoProperties(pluginConfig));

        if (pluginConfig.hasPath(SINK_LABEL_PREFIX.key())) {
            selectdbConfig.setLabelPrefix(pluginConfig.getString(SINK_LABEL_PREFIX.key()));
        } else {
            selectdbConfig.setLabelPrefix(SINK_LABEL_PREFIX.defaultValue());
        }
        if (pluginConfig.hasPath(SINK_MAX_RETRIES.key())) {
            selectdbConfig.setMaxRetries(pluginConfig.getInt(SINK_MAX_RETRIES.key()));
        } else {
            selectdbConfig.setMaxRetries(SINK_MAX_RETRIES.defaultValue());
        }
        if (pluginConfig.hasPath(SINK_ENABLE_2PC.key())) {
            selectdbConfig.setEnable2PC(pluginConfig.getBoolean(SINK_ENABLE_2PC.key()));
        } else {
            selectdbConfig.setEnable2PC(SINK_ENABLE_2PC.defaultValue());
        }
        if (pluginConfig.hasPath(SINK_BUFFER_SIZE.key())) {
            selectdbConfig.setBufferSize(pluginConfig.getInt(SINK_BUFFER_SIZE.key()));
        } else {
            selectdbConfig.setBufferSize(SINK_BUFFER_SIZE.defaultValue());
        }
        if (pluginConfig.hasPath(SINK_BUFFER_COUNT.key())) {
            selectdbConfig.setBufferCount(pluginConfig.getInt(SINK_BUFFER_COUNT.key()));
        } else {
            selectdbConfig.setBufferCount(SINK_BUFFER_COUNT.defaultValue());
        }
        if (pluginConfig.hasPath(SINK_ENABLE_DELETE.key())) {
            selectdbConfig.setEnableDelete(pluginConfig.getBoolean(SINK_ENABLE_DELETE.key()));
        } else {
            selectdbConfig.setEnableDelete(SINK_ENABLE_DELETE.defaultValue());
        }
        if (pluginConfig.hasPath(SINK_FLUSH_QUEUE_SIZE.key())) {
            selectdbConfig.setFlushQueueSize(pluginConfig.getInt(SINK_FLUSH_QUEUE_SIZE.key()));
        } else {
            selectdbConfig.setFlushQueueSize(SINK_FLUSH_QUEUE_SIZE.defaultValue());
        }
        return selectdbConfig;
    }

    private static Properties parseCopyIntoProperties(Config pluginConfig) {
        Properties stageLoadProps = new Properties();
        if (CheckConfigUtil.isValidParam(pluginConfig, SELECTDB_SINK_CONFIG_PREFIX.key())) {
            pluginConfig
                    .getObject(SELECTDB_SINK_CONFIG_PREFIX.key())
                    .forEach(
                            (key, value) -> {
                                final String configKey = key.toLowerCase();
                                stageLoadProps.put(configKey, value.unwrapped().toString());
                            });
        }
        return stageLoadProps;
    }
}
