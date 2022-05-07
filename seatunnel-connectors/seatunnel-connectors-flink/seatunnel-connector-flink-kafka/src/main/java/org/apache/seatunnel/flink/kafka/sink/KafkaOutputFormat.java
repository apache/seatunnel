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

import static org.apache.seatunnel.flink.kafka.Config.KAFKA_SINK_FORMAT_CSV_ARRAY_ELEMENT_DELIMITER;
import static org.apache.seatunnel.flink.kafka.Config.KAFKA_SINK_FORMAT_CSV_DISABLE_QUOTE_CHARACTER;
import static org.apache.seatunnel.flink.kafka.Config.KAFKA_SINK_FORMAT_CSV_ESCAPE_CHARACTER;
import static org.apache.seatunnel.flink.kafka.Config.KAFKA_SINK_FORMAT_CSV_FIELD_DELIMITER;
import static org.apache.seatunnel.flink.kafka.Config.KAFKA_SINK_FORMAT_CSV_LINE_DELIMITER;
import static org.apache.seatunnel.flink.kafka.Config.KAFKA_SINK_FORMAT_CSV_NULL_LITERAL;
import static org.apache.seatunnel.flink.kafka.Config.KAFKA_SINK_FORMAT_CSV_QUOTE_CHARACTER;
import static org.apache.seatunnel.flink.kafka.Config.KAFKA_SINK_FORMAT_TYPE;
import static org.apache.seatunnel.flink.kafka.sink.KafkaSinkConstants.KAFKA_PRODUCER_CLOSE_TIMEOUT;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.csv.CsvRowSerializationSchema;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaOutputFormat extends RichOutputFormat<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaOutputFormat.class);
    private final TypeInformation<Row> typeInfo;
    private final String topic;
    private final Map<String, String> formatProperties;
    private final Properties producerProperties;
    private final SinkFormatEnum formatEnum;
    private transient SerializationSchema<Row> serialization = null;
    private transient KafkaProducer<byte[], byte[]> producer;

    public KafkaOutputFormat(String topic, Properties producerProperties, Map<String, String> formatProperties, TypeInformation<Row> typeInfo) {
        this.topic = Objects.requireNonNull(topic, "kafka sink topic can not be null.");
        this.producerProperties = producerProperties;
        this.formatProperties = Objects.requireNonNull(formatProperties, "format properties can not be null, at least need 'format.type'.");
        this.typeInfo = Objects.requireNonNull(typeInfo, "Sink schema is null, this is maybe a bug, please report it.");
        this.formatEnum = SinkFormatEnum.fromName(formatProperties.get(KAFKA_SINK_FORMAT_TYPE));
    }

    @Override
    public void configure(Configuration configuration) {
        createSerializationSchema();
        createProducer();
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        // do nothing
    }

    @Override
    public void writeRecord(Row row) throws IOException {
        try {
            final byte[] serialize = serialization.serialize(row);
            producer.send(new ProducerRecord<>(topic, serialize), (metadata, e) -> {
                if (Objects.nonNull(e)) {
                    exceptionProcess(e, row);
                }
            });
        } catch (Exception e) {
            exceptionProcess(e, row);
        }
    }

    private void createSerializationSchema() {
        switch (formatEnum) {
            case CSV:
                final CsvRowSerializationSchema.Builder csvBuilder = new CsvRowSerializationSchema.Builder(typeInfo);

                Optional.ofNullable(formatProperties.get(KAFKA_SINK_FORMAT_CSV_LINE_DELIMITER)).ifPresent(csvBuilder::setLineDelimiter);
                Optional.ofNullable(formatProperties.get(KAFKA_SINK_FORMAT_CSV_FIELD_DELIMITER)).ifPresent(v -> csvBuilder.setFieldDelimiter(v.charAt(0)));

                final String disable = formatProperties.get(KAFKA_SINK_FORMAT_CSV_DISABLE_QUOTE_CHARACTER);
                if (Objects.nonNull(disable) && Boolean.parseBoolean(disable)) {
                    csvBuilder.disableQuoteCharacter();
                } else {
                    Optional.ofNullable(formatProperties.get(KAFKA_SINK_FORMAT_CSV_QUOTE_CHARACTER)).ifPresent(v -> csvBuilder.setQuoteCharacter(v.charAt(0)));
                }

                Optional.ofNullable(formatProperties.get(KAFKA_SINK_FORMAT_CSV_ARRAY_ELEMENT_DELIMITER)).ifPresent(csvBuilder::setArrayElementDelimiter);
                Optional.ofNullable(formatProperties.get(KAFKA_SINK_FORMAT_CSV_ESCAPE_CHARACTER)).ifPresent(v -> csvBuilder.setEscapeCharacter(v.charAt(0)));
                Optional.ofNullable(formatProperties.get(KAFKA_SINK_FORMAT_CSV_NULL_LITERAL)).ifPresent(csvBuilder::setNullLiteral);

                serialization = csvBuilder.build();
                break;
            case JSON:
                serialization = new JsonRowSerializationSchema.Builder(typeInfo).build();
                break;
            default:
                throw new RuntimeException("The type of format should be 'Json' or 'Csv' Only");
        }
    }

    private void createProducer() {
        producer = new KafkaProducer<>(producerProperties);
    }

    private void exceptionProcess(Exception e, Row record) {
        LOG.error("Write record to Kafka error, record is {}", record, e);
        throw new RuntimeException(e);
    }

    @Override
    public void close() throws IOException {
        LOG.warn("kafka output close.");
        // Set timeout to prevent blocking from close()
        if (Objects.nonNull(producer)) {
            producer.close(KAFKA_PRODUCER_CLOSE_TIMEOUT, TimeUnit.SECONDS);
        }
    }
}
