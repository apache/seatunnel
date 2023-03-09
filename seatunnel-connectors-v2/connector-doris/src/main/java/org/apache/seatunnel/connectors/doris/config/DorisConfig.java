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

package org.apache.seatunnel.connectors.doris.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.common.config.CheckConfigUtil;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;
import java.util.Properties;

@Setter
@Getter
@ToString
public class DorisConfig {
    public static final int DORIS_TABLET_SIZE_MIN = 1;
    public static final int DORIS_TABLET_SIZE_DEFAULT = Integer.MAX_VALUE;
    public static final int DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;
    public static final int DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;
    private static final int DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT = 3600;
    public static final int DORIS_REQUEST_RETRIES_DEFAULT = 3;
    private static final Boolean DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT = false;
    private static final int DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT = 64;
    private static final int DORIS_BATCH_SIZE_DEFAULT = 1024;
    private static final long DORIS_EXEC_MEM_LIMIT_DEFAULT = 2147483648L;
    private static final int DEFAULT_SINK_CHECK_INTERVAL = 10000;
    private static final int DEFAULT_SINK_MAX_RETRIES = 3;
    private static final int DEFAULT_SINK_BUFFER_SIZE = 256 * 1024;
    private static final int DEFAULT_SINK_BUFFER_COUNT = 3;
    // common option
    public static final Option<String> FENODES =
            Options.key("fenodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("doris fe http address.");

    public static final Option<String> TABLE_IDENTIFIER =
            Options.key("table.identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris table name.");
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris user name.");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris password.");

    // source config options
    public static final Option<String> DORIS_READ_FIELD =
            Options.key("doris.read.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of column names in the Doris table, separated by commas");
    public static final Option<String> DORIS_FILTER_QUERY =
            Options.key("doris.filter.query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Filter expression of the query, which is transparently transmitted to Doris. Doris uses this expression to complete source-side data filtering");
    public static final Option<Integer> DORIS_TABLET_SIZE =
            Options.key("doris.request.tablet.size")
                    .intType()
                    .defaultValue(DORIS_TABLET_SIZE_DEFAULT)
                    .withDescription("");
    public static final Option<Integer> DORIS_REQUEST_CONNECT_TIMEOUT_MS =
            Options.key("doris.request.connect.timeout.ms")
                    .intType()
                    .defaultValue(DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
                    .withDescription("");
    public static final Option<Integer> DORIS_REQUEST_READ_TIMEOUT_MS =
            Options.key("doris.request.read.timeout.ms")
                    .intType()
                    .defaultValue(DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
                    .withDescription("");
    public static final Option<Integer> DORIS_REQUEST_QUERY_TIMEOUT_S =
            Options.key("doris.request.query.timeout.s")
                    .intType()
                    .defaultValue(DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT)
                    .withDescription("");
    public static final Option<Integer> DORIS_REQUEST_RETRIES =
            Options.key("doris.request.retries")
                    .intType()
                    .defaultValue(DORIS_REQUEST_RETRIES_DEFAULT)
                    .withDescription("");
    public static final Option<Boolean> DORIS_DESERIALIZE_ARROW_ASYNC =
            Options.key("doris.deserialize.arrow.async")
                    .booleanType()
                    .defaultValue(DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
                    .withDescription("");
    public static final Option<Integer> DORIS_DESERIALIZE_QUEUE_SIZE =
            Options.key("doris.request.retriesdoris.deserialize.queue.size")
                    .intType()
                    .defaultValue(DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
                    .withDescription("");
    public static final Option<Integer> DORIS_BATCH_SIZE =
            Options.key("doris.batch.size")
                    .intType()
                    .defaultValue(DORIS_BATCH_SIZE_DEFAULT)
                    .withDescription("");
    public static final Option<Long> DORIS_EXEC_MEM_LIMIT =
            Options.key("doris.exec.mem.limit")
                    .longType()
                    .defaultValue(DORIS_EXEC_MEM_LIMIT_DEFAULT)
                    .withDescription("");
    public static final Option<Boolean> SOURCE_USE_OLD_API =
            Options.key("source.use-old-api")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to read data using the new interface defined according to the FLIP-27 specification,default false");

    // sink config options
    public static final Option<Boolean> SINK_ENABLE_2PC =
            Options.key("sink.enable-2pc")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("enable 2PC while loading");

    public static final Option<Integer> SINK_CHECK_INTERVAL =
            Options.key("sink.check-interval")
                    .intType()
                    .defaultValue(DEFAULT_SINK_CHECK_INTERVAL)
                    .withDescription("check exception with the interval while loading");
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
                    .defaultValue("")
                    .withDescription("the unique label prefix.");
    public static final Option<Boolean> SINK_ENABLE_DELETE =
            Options.key("sink.enable-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to enable the delete function");

    public static final Option<Map<String, String>> DORIS_SINK_CONFIG_PREFIX =
            Options.key("doris.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The parameter of the Stream Load data_desc. "
                                    + "The way to specify the parameter is to add the prefix `doris.config` to the original load parameter name ");

    // common option
    private String frontends;
    private String username;
    private String password;
    private String tableIdentifier;

    // source option
    private String readField;
    private String filterQuery;
    private Integer tabletSize;
    private Integer requestConnectTimeoutMs;
    private Integer requestReadTimeoutMs;
    private Integer requestQueryTimeoutS;
    private Integer requestRetries;
    private boolean deserializeArrowAsync;
    private int deserializeQueueSize;
    private int batchSize;
    private int execMemLimit;
    private boolean useOldApi;

    // sink option
    private Boolean enable2PC;
    private Boolean enableDelete;
    private String labelPrefix;
    private Integer checkInterval;
    private Integer maxRetries;
    private Integer bufferSize;
    private Integer bufferCount;
    private Properties streamLoadProps;

    public static DorisConfig loadConfig(Config pluginConfig) {
        DorisConfig dorisConfig = new DorisConfig();

        // common option
        dorisConfig.setFrontends(pluginConfig.getString(FENODES.key()));
        dorisConfig.setUsername(pluginConfig.getString(USERNAME.key()));
        dorisConfig.setPassword(pluginConfig.getString(PASSWORD.key()));
        dorisConfig.setTableIdentifier(pluginConfig.getString(TABLE_IDENTIFIER.key()));
        dorisConfig.setStreamLoadProps(parseStreamLoadProperties(pluginConfig));

        // source option
        if (pluginConfig.hasPath(DORIS_READ_FIELD.key())) {
            dorisConfig.setReadField(pluginConfig.getString(DORIS_READ_FIELD.key()));
        } else {
            dorisConfig.setReadField(DORIS_READ_FIELD.defaultValue());
        }
        if (pluginConfig.hasPath(DORIS_FILTER_QUERY.key())) {
            dorisConfig.setFilterQuery(pluginConfig.getString(DORIS_FILTER_QUERY.key()));
        } else {
            dorisConfig.setFilterQuery(DORIS_FILTER_QUERY.defaultValue());
        }
        if (pluginConfig.hasPath(DORIS_TABLET_SIZE.key())) {
            dorisConfig.setTabletSize(pluginConfig.getInt(DORIS_TABLET_SIZE.key()));
        } else {
            dorisConfig.setTabletSize(DORIS_TABLET_SIZE.defaultValue());
        }
        if (pluginConfig.hasPath(DORIS_REQUEST_CONNECT_TIMEOUT_MS.key())) {
            dorisConfig.setRequestReadTimeoutMs(
                    pluginConfig.getInt(DORIS_REQUEST_CONNECT_TIMEOUT_MS.key()));
        } else {
            dorisConfig.setRequestReadTimeoutMs(DORIS_REQUEST_CONNECT_TIMEOUT_MS.defaultValue());
        }
        if (pluginConfig.hasPath(DORIS_REQUEST_QUERY_TIMEOUT_S.key())) {
            dorisConfig.setRequestQueryTimeoutS(
                    pluginConfig.getInt(DORIS_REQUEST_QUERY_TIMEOUT_S.key()));
        } else {
            dorisConfig.setRequestQueryTimeoutS(DORIS_REQUEST_QUERY_TIMEOUT_S.defaultValue());
        }
        if (pluginConfig.hasPath(DORIS_REQUEST_READ_TIMEOUT_MS.key())) {
            dorisConfig.setRequestReadTimeoutMs(
                    pluginConfig.getInt(DORIS_REQUEST_READ_TIMEOUT_MS.key()));
        } else {
            dorisConfig.setRequestReadTimeoutMs(DORIS_REQUEST_READ_TIMEOUT_MS.defaultValue());
        }
        if (pluginConfig.hasPath(DORIS_REQUEST_RETRIES.key())) {
            dorisConfig.setRequestRetries(pluginConfig.getInt(DORIS_REQUEST_RETRIES.key()));
        } else {
            dorisConfig.setRequestRetries(DORIS_REQUEST_RETRIES.defaultValue());
        }
        if (pluginConfig.hasPath(DORIS_DESERIALIZE_ARROW_ASYNC.key())) {
            dorisConfig.setDeserializeArrowAsync(
                    pluginConfig.getBoolean(DORIS_DESERIALIZE_ARROW_ASYNC.key()));
        } else {
            dorisConfig.setDeserializeArrowAsync(DORIS_DESERIALIZE_ARROW_ASYNC.defaultValue());
        }
        if (pluginConfig.hasPath(DORIS_DESERIALIZE_QUEUE_SIZE.key())) {
            dorisConfig.setDeserializeQueueSize(
                    pluginConfig.getInt(DORIS_DESERIALIZE_QUEUE_SIZE.key()));
        } else {
            dorisConfig.setDeserializeQueueSize(DORIS_DESERIALIZE_QUEUE_SIZE.defaultValue());
        }
        if (pluginConfig.hasPath(DORIS_BATCH_SIZE.key())) {
            dorisConfig.setDeserializeQueueSize(pluginConfig.getInt(DORIS_BATCH_SIZE.key()));
        } else {
            dorisConfig.setDeserializeQueueSize(DORIS_BATCH_SIZE.defaultValue());
        }
        // sink option
        if (pluginConfig.hasPath(SINK_ENABLE_2PC.key())) {
            dorisConfig.setEnable2PC(pluginConfig.getBoolean(SINK_ENABLE_2PC.key()));
        } else {
            dorisConfig.setEnable2PC(SINK_ENABLE_2PC.defaultValue());
        }
        if (pluginConfig.hasPath(SINK_LABEL_PREFIX.key())) {
            dorisConfig.setLabelPrefix(pluginConfig.getString(SINK_LABEL_PREFIX.key()));
        } else {
            dorisConfig.setLabelPrefix(SINK_LABEL_PREFIX.defaultValue());
        }
        if (pluginConfig.hasPath(SINK_CHECK_INTERVAL.key())) {
            dorisConfig.setCheckInterval(pluginConfig.getInt(SINK_CHECK_INTERVAL.key()));
        } else {
            dorisConfig.setCheckInterval(SINK_CHECK_INTERVAL.defaultValue());
        }
        if (pluginConfig.hasPath(SINK_MAX_RETRIES.key())) {
            dorisConfig.setMaxRetries(pluginConfig.getInt(SINK_MAX_RETRIES.key()));
        } else {
            dorisConfig.setMaxRetries(SINK_MAX_RETRIES.defaultValue());
        }
        if (pluginConfig.hasPath(SINK_BUFFER_SIZE.key())) {
            dorisConfig.setBufferSize(pluginConfig.getInt(SINK_BUFFER_SIZE.key()));
        } else {
            dorisConfig.setBufferSize(SINK_BUFFER_SIZE.defaultValue());
        }
        if (pluginConfig.hasPath(SINK_BUFFER_COUNT.key())) {
            dorisConfig.setBufferCount(pluginConfig.getInt(SINK_BUFFER_COUNT.key()));
        } else {
            dorisConfig.setBufferCount(SINK_BUFFER_COUNT.defaultValue());
        }
        if (pluginConfig.hasPath(SINK_ENABLE_DELETE.key())) {
            dorisConfig.setEnableDelete(pluginConfig.getBoolean(SINK_ENABLE_DELETE.key()));
        } else {
            dorisConfig.setEnableDelete(SINK_ENABLE_DELETE.defaultValue());
        }
        return dorisConfig;
    }

    private static Properties parseStreamLoadProperties(Config pluginConfig) {
        Properties streamLoadProps = new Properties();
        if (CheckConfigUtil.isValidParam(pluginConfig, DORIS_SINK_CONFIG_PREFIX.key())) {
            pluginConfig
                    .getObject(DORIS_SINK_CONFIG_PREFIX.key())
                    .forEach(
                            (key, value) -> {
                                final String configKey = key.toLowerCase();
                                streamLoadProps.put(configKey, value.unwrapped().toString());
                            });
        }
        return streamLoadProps;
    }
}
