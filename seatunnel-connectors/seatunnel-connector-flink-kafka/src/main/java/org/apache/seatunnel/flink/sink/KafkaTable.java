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

package org.apache.seatunnel.flink.sink;

import org.apache.seatunnel.common.PropertiesUtil;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;
import org.apache.seatunnel.flink.util.SchemaUtil;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.Properties;

public class KafkaTable implements FlinkStreamSink<Row, Row> {

    private static final long serialVersionUID = 3980751499724935230L;
    private Config config;
    private Properties kafkaParams = new Properties();
    private String topic;

    @Override
    @Nullable
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        Table table = tableEnvironment.fromDataStream(dataStream);
        insert(tableEnvironment, table);
        return null;
    }

    private void insert(TableEnvironment tableEnvironment, Table table) {
        TypeInformation<?>[] types = table.getSchema().getFieldTypes();
        String[] fieldNames = table.getSchema().getFieldNames();
        Schema schema = getSchema(types, fieldNames);
        String uniqueTableName = SchemaUtil.getUniqueTableName();

        tableEnvironment.connect(getKafkaConnect())
                .withSchema(schema)
                .withFormat(setFormat())
                .inAppendMode()
                .createTemporaryTable(uniqueTableName);
        table.insertInto(uniqueTableName);
    }

    private Schema getSchema(TypeInformation<?>[] informations, String[] fieldNames) {
        Schema schema = new Schema();
        for (int i = 0; i < informations.length; i++) {
            schema.field(fieldNames[i], informations[i]);
        }
        return schema;
    }

    private Kafka getKafkaConnect() {

        Kafka kafka = new Kafka().version("universal");
        kafka.topic(topic);
        kafka.properties(kafkaParams);
        return kafka;
    }

    private FormatDescriptor setFormat() {
        return new Json().failOnMissingField(false).deriveSchema();
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
        String producerPrefix = "producer.";
        PropertiesUtil.setProperties(config, kafkaParams, producerPrefix, false);
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    }
}
