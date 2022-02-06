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

package org.apache.seatunnel.spark;

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.env.RuntimeEnv;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;

public class SparkEnvironment implements RuntimeEnv {

    private static final long DEFAULT_SPARK_STREAMING_DURATION = 5;

    private SparkSession sparkSession;

    private StreamingContext streamingContext;

    private Config config = ConfigFactory.empty();

    @Override
    public void setConfig(Config config) {
        this.config = config;
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
    public void prepare(Boolean prepareEnv) {
        SparkConf sparkConf = createSparkConf();
        this.sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        createStreamingContext();
    }

    public SparkSession getSparkSession() {
        return this.sparkSession;
    }

    public StreamingContext getStreamingContext() {
        return this.streamingContext;
    }

    private SparkConf createSparkConf() {
        SparkConf sparkConf = new SparkConf();
        this.config.entrySet().forEach(entry -> sparkConf.set(entry.getKey(), String.valueOf(entry.getValue().unwrapped())));
        return sparkConf;
    }

    private void createStreamingContext() {
        SparkConf conf = this.sparkSession.sparkContext().getConf();
        long duration = conf.getLong("spark.stream.batchDuration", DEFAULT_SPARK_STREAMING_DURATION);
        if (this.streamingContext == null) {
            this.streamingContext = new StreamingContext(sparkSession.sparkContext(), Seconds.apply(duration));
        }
    }
}
