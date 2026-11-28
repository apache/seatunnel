/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.harvest;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.constants.JobMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

class HarvestSourceFactoryTest {
    static final String TOKEN = "test-harvest-secret";

    static Map<String, Object> options() {
        Map<String, Object> options = new HashMap<>();
        options.put("token", TOKEN);
        options.put("account_id", "12345");
        Map<String, String> fields = new java.util.LinkedHashMap<>();
        fields.put("id", "bigint");
        fields.put("notes", "string");
        fields.put("hours", "decimal(18,4)");
        fields.put("spent_date", "date");
        options.put("schema", Collections.singletonMap("fields", fields));
        return options;
    }

    @Test
    void createsSourceFromParsedJobConfiguration() {
        ReadonlyConfig config =
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString(
                                        "source { Harvest {\n"
                                                + "token = test-token\n"
                                                + "account_id = 12345\n"
                                                + "schema { fields { id = bigint } }\n"
                                                + "} }")
                                .getConfigList("source")
                                .get(0));
        HarvestSourceFactory factory = new HarvestSourceFactory();
        ConfigValidator.of(config).validate(factory.optionRule());
        SeaTunnelSource<?, ?, ?> source =
                factory.createSource(
                                new TableSourceFactoryContext(config, getClass().getClassLoader()))
                        .createSource();
        Assertions.assertArrayEquals(
                new String[] {"id"},
                source.getProducedCatalogTables().get(0).getSeaTunnelRowType().getFieldNames());
    }

    @Test
    void discoversFactoryAndProducesConfiguredSchema() {
        Assertions.assertTrue(
                StreamSupport.stream(ServiceLoader.load(Factory.class).spliterator(), false)
                        .anyMatch(f -> f instanceof HarvestSourceFactory));
        HarvestSource source = new HarvestSource(ReadonlyConfig.fromMap(options()));
        Assertions.assertEquals("Harvest", source.getPluginName());
        Assertions.assertEquals(Boundedness.BOUNDED, source.getBoundedness());
        Assertions.assertArrayEquals(
                new String[] {"id", "notes", "hours", "spent_date"},
                source.getProducedCatalogTables().get(0).getSeaTunnelRowType().getFieldNames());
        JobContext context = new JobContext();
        context.setJobMode(JobMode.STREAMING);
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> source.setJobContext(context));
        SourceReader.Context reader = Mockito.mock(SourceReader.Context.class);
        Mockito.when(reader.getIndexOfSubtask()).thenReturn(1);
        Assertions.assertThrows(IllegalArgumentException.class, () -> source.createReader(reader));
    }

    @ParameterizedTest
    @CsvSource({
        "page_size,0",
        "page_size,2001",
        "retry,-1",
        "retry_backoff_ms,0",
        "max_retry_delay_ms,0",
        "connect_timeout_ms,0",
        "socket_timeout_ms,0",
        "from,2026-02-30",
        "to,bad-date",
        "updated_since,2026-01-01T00:00:00",
        "account_id,abc",
        "base_url,https://example.com/path",
        "base_url,https://user:secret@example.com",
        "base_url,https://example.com?token=secret",
        "base_url,ftp://example.com"
    })
    void rejectsInvalidOptionsWithoutPrintingSecrets(String key, String value) {
        Map<String, Object> options = options();
        options.put(key, value);
        Exception error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new HarvestSource(ReadonlyConfig.fromMap(options)));
        Assertions.assertTrue(error.getMessage().contains(key));
        Assertions.assertFalse(error.toString().contains(TOKEN));
    }

    @Test
    void rejectsBlankCredentialsAndReversedDateRange() {
        for (String key : new String[] {"token", "account_id", "from"}) {
            Map<String, Object> options = options();
            options.put(key, " ");
            Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> new HarvestSource(ReadonlyConfig.fromMap(options)));
        }
        Map<String, Object> options = options();
        options.put("from", "2026-11-02");
        options.put("to", "2026-11-01");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new HarvestSource(ReadonlyConfig.fromMap(options)));
        final Map<String, Object> missingSchema = options();
        missingSchema.remove("schema");
        Assertions.assertThrows(
                Exception.class, () -> new HarvestSource(ReadonlyConfig.fromMap(missingSchema)));
    }
}
