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

package org.apache.seatunnel.trace.collector.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class TraceCollectorConfigTest {

    @Test
    void testDefaultDbSchemaForMySqlIsEmpty() throws Exception {
        Path temp = Files.createTempFile("trace-collector-mysql", ".properties");
        Files.write(
                temp,
                ("db.type=mysql\n"
                                + "db.jdbcUrl=jdbc:mysql://127.0.0.1:3306/seatunnel_trace\n"
                                + "db.username=u\n"
                                + "db.password=p\n")
                        .getBytes(StandardCharsets.UTF_8));
        String old = System.getProperty("trace.collector.config");
        try {
            System.setProperty("trace.collector.config", temp.toString());
            TraceCollectorConfig cfg = TraceCollectorConfig.load();
            Assertions.assertTrue(cfg.getDbSchema() == null || cfg.getDbSchema().isEmpty());
        } finally {
            if (old == null) {
                System.clearProperty("trace.collector.config");
            } else {
                System.setProperty("trace.collector.config", old);
            }
            Files.deleteIfExists(temp);
        }
    }

    @Test
    void testDefaultDbSchemaForPostgresIsPublic() throws Exception {
        Path temp = Files.createTempFile("trace-collector-postgres", ".properties");
        Files.write(
                temp,
                ("db.type=postgres\n"
                                + "db.jdbcUrl=jdbc:postgresql://127.0.0.1:5432/seatunnel\n"
                                + "db.username=u\n"
                                + "db.password=p\n")
                        .getBytes(StandardCharsets.UTF_8));
        String old = System.getProperty("trace.collector.config");
        try {
            System.setProperty("trace.collector.config", temp.toString());
            TraceCollectorConfig cfg = TraceCollectorConfig.load();
            Assertions.assertEquals("public", cfg.getDbSchema());
        } finally {
            if (old == null) {
                System.clearProperty("trace.collector.config");
            } else {
                System.setProperty("trace.collector.config", old);
            }
            Files.deleteIfExists(temp);
        }
    }
}
