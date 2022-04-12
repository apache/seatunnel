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

package org.apache.seatunnel.spark.http.source;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.spark.SparkEnvironment;
import org.apache.seatunnel.spark.batch.SparkBatchSource;
import org.apache.seatunnel.spark.http.source.constant.Settings;
import org.apache.seatunnel.spark.http.source.util.HttpClientResult;
import org.apache.seatunnel.spark.http.source.util.HttpClientUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Http extends SparkBatchSource {

    private static final String GET = "GET";
    private static final String POST = "POST";
    private static final int INITIAL_CAPACITY = 16;

    private static Logger LOG = LoggerFactory.getLogger(Http.class);

    @Override
    public CheckResult checkConfig() {
        return CheckConfigUtil.checkAllExists(config, "url");
    }

    @Override
    public void prepare(SparkEnvironment prepareEnv) {
    }

    @Override
    public Dataset<Row> getData(SparkEnvironment env) {
        SparkSession spark = env.getSparkSession();
        String url = config.getString(Settings.SOURCE_HTTP_URL);
        String method = TypesafeConfigUtils.getConfig(config, Settings.SOURCE_HTTP_METHOD, GET);
        String header = TypesafeConfigUtils.getConfig(config, Settings.SOURCE_HTTP_HEADER, "");
        String requestParams = TypesafeConfigUtils.getConfig(config, Settings.SOURCE_HTTP_REQUEST_PARAMS, "");
        String syncPath = TypesafeConfigUtils.getConfig(config, Settings.SOURCE_HTTP_SYNC_PATH, "");

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Map requestMap = jsonToMap(requestParams);
        String syncValues = getSyncValues(jsc, syncPath);
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
        }

        LOG.info("http respond code->{}", response.getCode());

        List<String> array = new ArrayList<>();
        array.add(response.getContent());
        JavaRDD<String> javaRDD = jsc.parallelize(array);
        DataFrameReader reader = spark.read().format("json");
        return reader.json(javaRDD);
    }

    private String getSyncValues(JavaSparkContext jsc, String syncPath) {
        if (null == syncPath || syncPath.isEmpty()) {
            return "";
        }
        Configuration hadoopConf = jsc.hadoopConfiguration();
        List<String> values = new ArrayList<>();
        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            Path path = new Path(syncPath);
            boolean exists = fs.exists(path);
            if (exists) {
                JavaRDD<String> checkPoint = jsc.textFile(syncPath);
                values = checkPoint.collect();

            }
        } catch (IOException e) {
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
