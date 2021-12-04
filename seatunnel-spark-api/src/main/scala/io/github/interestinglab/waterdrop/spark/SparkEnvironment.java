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

package io.github.interestinglab.waterdrop.spark;

import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.config.ConfigFactory;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;

public class SparkEnvironment implements RuntimeEnv {
    private SparkSession sparkSession;

    private StreamingContext streamingContext;

    public Config config = ConfigFactory.empty();

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
        return new CheckResult(true, "");
    }

    @Override
    public void prepare(Boolean prepareEnv) {
        SparkConf sparkConf = createSparkConf();
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        createStreamingContext();
    }

    private SparkConf createSparkConf() {
        SparkConf sparkConf = new SparkConf();
        config
                .entrySet()
                .forEach(entry -> {
                    sparkConf.set(entry.getKey(), String.valueOf(entry.getValue().unwrapped()));
                });

        return sparkConf;
    }

    private StreamingContext createStreamingContext() {
        SparkConf conf = sparkSession.sparkContext().getConf();
        long duration = conf.getLong("spark.stream.batchDuration", 5);
        if (streamingContext == null) {

            streamingContext =
                    new StreamingContext(sparkSession.sparkContext(), Durations.seconds(duration));
        }
        return streamingContext;
    }

    public StreamingContext getStreamingContext() {
        return streamingContext;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }
}
