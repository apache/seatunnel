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

package org.apache.seatunnel.e2e.spark.v2.kafka;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.DefaultSeaTunnelRowSerializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;

@Slf4j
public class KafkaSourceStartConfigToConsoleIT extends KafkaTestBaseIT {
    @Override
    protected void generateTestData() {
        generateStepTestData(0, 100);
    }

    private void generateStepTestData(int start, int end) {

        SeaTunnelRowType seatunnelRowType = new SeaTunnelRowType(
            new String[]{
                "id"
            },
            new SeaTunnelDataType[]{
                BasicType.LONG_TYPE
            }
        );

        DefaultSeaTunnelRowSerializer serializer = new DefaultSeaTunnelRowSerializer("test_topic", seatunnelRowType);
        for (int i = start; i < end; i++) {
            SeaTunnelRow row = new SeaTunnelRow(
                new Object[]{
                    Long.valueOf(i)
                });
            ProducerRecord<byte[], byte[]> producerRecord = serializer.serializeRow(row);
            producer.send(producerRecord);
        }
    }

    @Test
    public void testKafka() throws IOException, InterruptedException {
        testKafkaLatestToConsole();
        testKafkaEarliestToConsole();
        testKafkaSpecificOffsetsToConsole();
        testKafkaGroupOffsetsToConsole();
        testKafkaTimestampToConsole();
    }

    public void testKafkaLatestToConsole() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/kafka/kafkasource_latest_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    public void testKafkaEarliestToConsole() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/kafka/kafkasource_earliest_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    public void testKafkaSpecificOffsetsToConsole() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/kafka/kafkasource_specific_offsets_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    public void testKafkaGroupOffsetsToConsole() throws IOException, InterruptedException {
        generateStepTestData(100, 150);
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/kafka/kafkasource_group_offset_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    public void testKafkaTimestampToConsole() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/kafka/kafkasource_timestamp_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

}
