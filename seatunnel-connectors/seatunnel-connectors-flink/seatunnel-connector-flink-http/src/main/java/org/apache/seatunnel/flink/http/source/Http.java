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

package org.apache.seatunnel.flink.http.source;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;
import org.apache.seatunnel.flink.http.source.constant.Settings;
import org.apache.seatunnel.flink.http.source.util.HttpClientResult;
import org.apache.seatunnel.flink.http.source.util.HttpClientUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Http implements FlinkBatchSource {

    private static final String GET = "GET";
    private static final String POST = "POST";
    private static final int INITIAL_CAPACITY = 16;

    private static Logger LOG = LoggerFactory.getLogger(Http.class);

    private Config config;
    private String url;
    private String method;
    private String header;
    private String requestParams;
    private String syncPath;
    private Map requestMap;
    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckConfigUtil.checkAllExists(config, Settings.SOURCE_HTTP_URL);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        url = config.getString(Settings.SOURCE_HTTP_URL);
        method = TypesafeConfigUtils.getConfig(config, Settings.SOURCE_HTTP_METHOD, GET);
        header = TypesafeConfigUtils.getConfig(config, Settings.SOURCE_HTTP_HEADER, "");
        requestParams = TypesafeConfigUtils.getConfig(config, Settings.SOURCE_HTTP_REQUEST_PARAMS, "");
        syncPath = TypesafeConfigUtils.getConfig(config, Settings.SOURCE_HTTP_SYNC_PATH, "");

        requestMap = jsonToMap(requestParams);
    }

    @Override
    public DataSet<Row> getData(FlinkEnvironment env) {
        String syncValues = getSyncValues(env.getBatchEnvironment(), syncPath);
        LOG.info("sync values->{}", syncValues);
        Map syncMap = jsonToMap(syncValues);
        if (!syncMap.isEmpty()) {
            requestMap.putAll(syncMap);
        }

        HttpClientResult response = new HttpClientResult();
        try {
            Map headerMap = jsonToMap(header);
            if (POST.equals(method)) {
                response = HttpClientUtils.doPost(url, headerMap, requestMap);
            } else {
                response = HttpClientUtils.doGet(url, headerMap, requestMap);
            }
        } catch (Exception e) {
            LOG.error("http call error!", e);
            throw new RuntimeException(e);
        }

        LOG.info("http respond code->{}", response.getCode());
        LOG.info("http respond body->{}", response.getContent());

        return env.getBatchTableEnvironment().toDataSet(
            env.getBatchTableEnvironment().fromValues(
                DataTypes.ROW(DataTypes.FIELD("rawMsg", DataTypes.STRING())),
                response.getContent()
            ),
            Row.class
        );
    }

    private String getSyncValues(ExecutionEnvironment env, String syncPath) {
        if (null == syncPath || syncPath.isEmpty()) {
            return "";
        }

        List<String> values = new ArrayList<>();
        try {
            DataSource<String> source = env.readTextFile(syncPath);
            if (null != source) {
                values = source.collect();
            }
        } catch (Exception e) {
            LOG.error("getSyncValues error, syncPath is {}", syncPath, e);
        }
        return values.isEmpty() ? "" : values.iterator().next();
    }

    private Map jsonToMap(String content) {
        Map map = new HashMap<>(INITIAL_CAPACITY);
        if (null == content || content.isEmpty()) {
            return map;
        }

        try {
            return new ObjectMapper().readValue(content, HashMap.class);
        } catch (IOException e) {
            //only records the log, does not handle it, and does not affect the main process.
            LOG.error("{} json to map error!", content, e);
        }
        return map;
    }
}
