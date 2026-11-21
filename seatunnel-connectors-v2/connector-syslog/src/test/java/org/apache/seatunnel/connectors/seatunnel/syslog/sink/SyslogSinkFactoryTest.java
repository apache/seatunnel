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

package org.apache.seatunnel.connectors.seatunnel.syslog.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.connectors.seatunnel.syslog.config.SyslogSinkConfig;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SyslogSinkFactoryTest {
    @Test
    void validatesDefaultsAndNumericBoundaries() {
        new SyslogSinkConfig(ReadonlyConfig.fromMap(config()));
        for (String name :
                new String[] {
                    "port", "connect_timeout_ms", "write_timeout_ms", "max_message_bytes"
                }) {
            Map<String, Object> values = config();
            values.put(name, 1);
            new SyslogSinkConfig(ReadonlyConfig.fromMap(values));
            values.put(name, 0);
            OptionValidationException error =
                    assertThrows(
                            OptionValidationException.class,
                            () -> new SyslogSinkConfig(ReadonlyConfig.fromMap(values)));
            assertTrue(error.getMessage().contains(name));
        }
        for (String name : new String[] {"port", "max_message_bytes"}) {
            Map<String, Object> values = config();
            values.put(name, name.equals("port") ? 65536 : 1048577);
            assertThrows(
                    OptionValidationException.class,
                    () -> new SyslogSinkConfig(ReadonlyConfig.fromMap(values)));
        }
    }

    @Test
    void rejectsMissingHostAndInvalidTlsOptions() {
        assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(Collections.emptyMap()))
                                .validate(new SyslogSinkFactory().optionRule()));
        for (String host : new String[] {"", " ", "local\nhost", " localhost"}) {
            Map<String, Object> values = config();
            values.put("host", host);
            assertThrows(
                    RuntimeException.class,
                    () -> new SyslogSinkConfig(ReadonlyConfig.fromMap(values)));
        }
        for (String key : new String[] {"tls.key_store.path", "password", "tls.key_store.type"}) {
            Map<String, Object> values = config();
            values.put(key, "not-for-diagnostics");
            IllegalArgumentException error =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> new SyslogSinkConfig(ReadonlyConfig.fromMap(values)));
            assertFalse(error.getMessage().contains("not-for-diagnostics"));
        }
    }

    @Test
    void discoversFactoryAndSerializesSinkWithoutOpeningTlsFilesOrNetwork() {
        assertTrue(
                StreamSupport.stream(ServiceLoader.load(Factory.class).spliterator(), false)
                        .anyMatch(factory -> factory instanceof SyslogSinkFactory));
        Map<String, Object> values = config();
        values.put("host", "unresolved.invalid");
        values.put("tls.ca_cert_path", "not-read-until-worker.pem");
        SyslogSink original =
                (SyslogSink)
                        new SyslogSinkFactory()
                                .createSink(
                                        new TableSinkFactoryContext(
                                                table(),
                                                ReadonlyConfig.fromMap(values),
                                                getClass().getClassLoader()))
                                .createSink();
        SyslogSink restored =
                SerializationUtils.deserialize(SerializationUtils.serialize(original));
        assertEquals("Syslog", restored.getPluginName());
        assertEquals(
                "message",
                restored.getWriteCatalogTable().get().getSeaTunnelRowType().getFieldNames()[0]);
    }

    static Map<String, Object> config() {
        Map<String, Object> values = new HashMap<>();
        values.put("host", "localhost");
        return values;
    }

    static CatalogTable table() {
        return CatalogTable.of(
                TableIdentifier.of("default", "default", "syslog"),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "message", BasicType.STRING_TYPE, 0, true, null, null))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "Syslog");
    }
}
