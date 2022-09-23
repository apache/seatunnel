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

package org.apache.seatunnel.example.flink.v2;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.core.starter.Seatunnel;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.command.FlinkCommandBuilder;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class HttpConnectorExample {

    private static final SeaTunnelRowType ROW_TYPE = new SeaTunnelRowType(
        new String[]{
            "id",
            "c_map",
            "c_array",
            "c_string",
            "c_boolean",
            "c_tinyint",
            "c_smallint",
            "c_int",
            "c_bigint",
            "c_float",
            "c_double",
            "c_decimal",
            "c_bytes",
            "c_date",
            "c_timestamp"
        },
        new SeaTunnelDataType[]{
            BasicType.LONG_TYPE,
            new MapType(BasicType.STRING_TYPE, BasicType.SHORT_TYPE),
            ArrayType.BYTE_ARRAY_TYPE,
            BasicType.STRING_TYPE,
            BasicType.BOOLEAN_TYPE,
            BasicType.BYTE_TYPE,
            BasicType.SHORT_TYPE,
            BasicType.INT_TYPE,
            BasicType.LONG_TYPE,
            BasicType.FLOAT_TYPE,
            BasicType.DOUBLE_TYPE,
            new DecimalType(2, 1),
            PrimitiveByteArrayType.INSTANCE,
            LocalTimeType.LOCAL_DATE_TYPE,
            LocalTimeType.LOCAL_DATE_TIME_TYPE
        });
    private static final List<SeaTunnelRow> TEST_DATASET = generateTestDataset();
    private static final JsonSerializationSchema SERIALIZATION_SCHEMA = new JsonSerializationSchema(ROW_TYPE);
    private static final JsonDeserializationSchema DESERIALIZATION_SCHEMA = new JsonDeserializationSchema(
        false, false, ROW_TYPE);
    private static final int MOCK_WEB_SERVER_PORT = 18888;

    public static void main(String[] args) throws Exception {
        BlockingQueue<String> readDataQueue = new LinkedBlockingQueue<>();
        BlockingQueue<String> writeDataQueue = new LinkedBlockingQueue<>();

        String configFile = "/examples/http_source_to_sink.conf";
        try (MockWebServer mockWebServer = startWebServer(MOCK_WEB_SERVER_PORT, readDataQueue, writeDataQueue)) {
            log.info("Submitting http job: {}", configFile);
            readDataQueue.offer(toJson(TEST_DATASET));
            submitHttpJob(configFile);
            log.info("Submitted http job: {}", configFile);

            log.info("Validate http sink data...");
            checkArgument(writeDataQueue.size() == TEST_DATASET.size());
            List<SeaTunnelRow> results = new ArrayList<>();
            for (int i = 0; i < TEST_DATASET.size(); i++) {
                SeaTunnelRow row = DESERIALIZATION_SCHEMA.deserialize(writeDataQueue.take().getBytes());
                results.add(row);
            }
            checkArgument(results.equals(TEST_DATASET));
        }
    }

    private static void submitHttpJob(String configurePath) throws Exception {
        String configFile = getTestConfigFile(configurePath);
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setConfigFile(configFile);
        flinkCommandArgs.setCheckConfig(false);
        flinkCommandArgs.setVariables(null);
        Command<FlinkCommandArgs> flinkCommand =
            new FlinkCommandBuilder().buildCommand(flinkCommandArgs);
        Seatunnel.run(flinkCommand);
    }

    private static String getTestConfigFile(String configFile) throws FileNotFoundException, URISyntaxException {
        URL resource = HttpConnectorExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }

    private static MockWebServer startWebServer(int port,
                                                BlockingQueue<String> readDataQueue,
                                                BlockingQueue<String> writeDataQueue) throws Exception {
        MockWebServer mockWebServer = new MockWebServer();
        mockWebServer.start(InetAddress.getByName("localhost"), port);
        mockWebServer.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                log.info("received request : {}", request.getPath());
                if (request.getPath().endsWith("/read")) {
                    if (readDataQueue.isEmpty()) {
                        return new MockResponse();
                    }
                    String readData = readDataQueue.take();
                    log.info("Take readDataQueue, remaining capacity: {}", readDataQueue.size());
                    return new MockResponse().setBody(readData);
                }
                if (request.getPath().endsWith("/write")) {
                    writeDataQueue.offer(request.getUtf8Body());
                    log.info("Offer writeDataQueue, remaining capacity: {}", writeDataQueue.size());
                    return new MockResponse().setBody("write request");
                }
                return new MockResponse();
            }
        });
        log.info("Started MockWebServer: {}", mockWebServer.url("/"));
        return mockWebServer;
    }

    @SuppressWarnings("MagicNumber")
    private static List<SeaTunnelRow> generateTestDataset() {
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row = new SeaTunnelRow(new Object[]{
                Long.valueOf(i),
                Collections.singletonMap("key", Short.parseShort("1")),
                new Byte[]{Byte.parseByte("1")},
                "string",
                Boolean.FALSE,
                Byte.parseByte("1"),
                Short.parseShort("1"),
                Integer.parseInt("1"),
                Long.parseLong("1"),
                Float.parseFloat("1.1"),
                Double.parseDouble("1.1"),
                BigDecimal.valueOf(11, 1),
                "test".getBytes(),
                LocalDate.now(),
                LocalDateTime.now()
            });
            rows.add(row);
        }
        return rows;
    }

    private static String toJson(List<SeaTunnelRow> rows) throws IOException {
        ArrayNode arrayNode = SERIALIZATION_SCHEMA.getMapper().createArrayNode();
        for (SeaTunnelRow row : rows) {
            byte[] jsonBytes = SERIALIZATION_SCHEMA.serialize(row);
            JsonNode jsonNode = SERIALIZATION_SCHEMA.getMapper().readTree(jsonBytes);
            arrayNode.add(jsonNode);
        }
        return SERIALIZATION_SCHEMA.getMapper().writeValueAsString(arrayNode);
    }
}
