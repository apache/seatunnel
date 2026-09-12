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

package org.apache.seatunnel.core.starter.spark.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.execution.RuntimeEnvironment;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.List;

@Slf4j
public class SparkRuntimeEnvironment implements RuntimeEnvironment {
    private static final long DEFAULT_SPARK_STREAMING_DURATION = 5;
    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private static volatile SparkRuntimeEnvironment INSTANCE = null;

    private SparkConf sparkConf;

    private SparkSession sparkSession;

    private StreamingContext streamingContext;

    private Config config;

    private boolean enableHive = false;

    private JobMode jobMode;

    private String jobName = Constants.LOGO;

    private SparkRuntimeEnvironment(Config config) {
        this.setEnableHive(checkIsContainHive(config));
        this.initialize(config);
    }

    public void setEnableHive(boolean enableHive) {
        this.enableHive = enableHive;
    }

    @Override
    public RuntimeEnvironment setConfig(Config config) {
        this.config = config;
        return this;
    }

    @Override
    public RuntimeEnvironment setJobMode(JobMode mode) {
        this.jobMode = mode;
        return this;
    }

    @Override
    public JobMode getJobMode() {
        return jobMode;
    }

    @Override
    public Config getConfig() {
        return this.config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckResult.success();
    }

    @Override
    public void registerPlugin(List<URL> pluginPaths) {
        log.info("register plugins :" + pluginPaths);
        // TODO we use --jar parameter to support submit multi-jar in spark cluster at now. Refactor
        // it to
        //  support submit multi-jar in code or remove this logic.
        // this.sparkSession.conf().set("spark.jars",pluginPaths.stream().map(URL::getPath).collect(Collectors.joining(",")));
    }

    @Override
    public SparkRuntimeEnvironment prepare() {
        if (config.hasPath("job.name")) {
            this.jobName = config.getString("job.name");
        }
        sparkConf = createSparkConf();
        SparkSession.Builder builder = SparkSession.builder().config(sparkConf);
        if (enableHive) {
            builder.enableHiveSupport();
        }
        this.sparkSession = builder.getOrCreate();
        createStreamingContext();
        return this;
    }

    public SparkSession getSparkSession() {
        return this.sparkSession;
    }

    public StreamingContext getStreamingContext() {
        return this.streamingContext;
    }

    public SparkConf getSparkConf() {
        return this.sparkConf;
    }

    private SparkConf createSparkConf() {
        SparkConf sparkConf = new SparkConf();
        this.config
                .entrySet()
                .forEach(
                        entry ->
                                sparkConf.set(
                                        entry.getKey(),
                                        String.valueOf(entry.getValue().unwrapped())));
        sparkConf.setAppName(jobName);
        return sparkConf;
    }

    private void createStreamingContext() {
        SparkConf conf = this.sparkSession.sparkContext().getConf();
        long duration =
                conf.getLong("spark.stream.batchDuration", DEFAULT_SPARK_STREAMING_DURATION);
        if (this.streamingContext == null) {
            this.streamingContext =
                    new StreamingContext(sparkSession.sparkContext(), Seconds.apply(duration));
        }
    }

    protected boolean checkIsContainHive(Config config) {
        List<? extends Config> sourceConfigList = config.getConfigList(PluginType.SOURCE.getType());
        for (Config c : sourceConfigList) {
            if (c.getString(PLUGIN_NAME_KEY).toLowerCase().contains("hive")) {
                return true;
            }
        }
        List<? extends Config> sinkConfigList = config.getConfigList(PluginType.SINK.getType());
        for (Config c : sinkConfigList) {
            if (c.getString(PLUGIN_NAME_KEY).toLowerCase().contains("hive")) {
                return true;
            }
        }
        return false;
    }

    public static SparkRuntimeEnvironment getInstance(Config config) {
        if (INSTANCE == null) {
            synchronized (SparkRuntimeEnvironment.class) {
                if (INSTANCE == null) {
                    INSTANCE = new SparkRuntimeEnvironment(config);
                }
            }
        }
        return INSTANCE;
    }
}
