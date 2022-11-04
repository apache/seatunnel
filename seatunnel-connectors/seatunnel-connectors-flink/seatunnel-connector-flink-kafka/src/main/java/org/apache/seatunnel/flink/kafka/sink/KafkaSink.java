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

package org.apache.seatunnel.flink.kafka.sink;

import org.apache.seatunnel.common.PropertiesUtil;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.BaseFlinkSink;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.types.Row;

import java.util.Properties;

@AutoService(BaseFlinkSink.class)
public class KafkaSink implements FlinkStreamSink {
    private static final long serialVersionUID = 3980751499724935230L;
    private static final String DEFAULT_KAFKA_SEMANTIC = "at_least_once";
    private Config config;
    private Properties kafkaParams = new Properties();
    private String topic;
    private String semantic = DEFAULT_KAFKA_SEMANTIC;

    @Override
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        FlinkKafkaProducer<Row> rowFlinkKafkaProducer = new FlinkKafkaProducer<>(
                topic,
                JsonRowSerializationSchema.builder().withTypeInfo(dataStream.getType()).build(),
                kafkaParams,
                null,
                getSemanticEnum(semantic),
                FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);
        dataStream.addSink(rowFlinkKafkaProducer);
    }

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
        return CheckConfigUtil.checkAllExists(config, "topics");
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        topic = config.getString("topics");
        if (config.hasPath("semantic")) {
            semantic = config.getString("semantic");
        }
        String producerPrefix = "producer.";
        PropertiesUtil.setProperties(config, kafkaParams, producerPrefix, false);
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    }

    @Override
    public String getPluginName() {
        return "Kafka";
    }

    private FlinkKafkaProducer.Semantic getSemanticEnum(String semantic) {
        if ("exactly_once".equals(semantic)) {
            return FlinkKafkaProducer.Semantic.EXACTLY_ONCE;
        } else if ("at_least_once".equals(semantic)) {
            return FlinkKafkaProducer.Semantic.AT_LEAST_ONCE;
        } else {
            return FlinkKafkaProducer.Semantic.NONE;
        }
    }
}
