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

package mongodb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.MongodbIncrementalSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfigProvider;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffsetFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongodbIncrementalSourceFactoryTest {

    private final OptionRule rule = new MongodbIncrementalSourceFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("hosts", "localhost:27017");
        cfg.put("database", Collections.singletonList("testdb"));
        cfg.put("collection", Collections.singletonList("testcol"));
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", Collections.singletonMap("id", "int"));
        cfg.put("schema", schema);
        return cfg;
    }

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new MongodbIncrementalSourceFactory()).optionRule());
    }

    @Test
    public void testSupportedStartUpModes() {
        MongodbIncrementalSourceFactory mongodbIncrementalSourceFactory =
                new MongodbIncrementalSourceFactory();
        mongodbIncrementalSourceFactory.optionRule().getOptionalOptions().stream()
                .filter((option) -> option.key().equals(SourceOptions.STARTUP_MODE_KEY))
                .forEach(
                        (option) -> {
                            Assertions.assertIterableEquals(
                                    Arrays.asList(
                                            StartupMode.INITIAL,
                                            StartupMode.LATEST,
                                            StartupMode.TIMESTAMP),
                                    ((SingleChoiceOption<StartupMode>) option).getOptionValues());
                        });
    }

    @Test
    public void testNumericOptionsWithValidValues() {
        Map<String, Object> cfg = validConfig();
        cfg.put("batch.size", 0);
        cfg.put("poll.await.time.ms", 1);
        cfg.put("poll.max.batch.size", 512);
        cfg.put("heartbeat.interval.ms", 0);
        cfg.put("incremental.snapshot.chunk.size.mb", 1);
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    public void testNumericOptionsWithInvalidValues() {
        Map<String, Object> cfg1 = validConfig();
        cfg1.put("batch.size", -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg1));

        Map<String, Object> cfg2 = validConfig();
        cfg2.put("poll.await.time.ms", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg2));

        Map<String, Object> cfg3 = validConfig();
        cfg3.put("poll.max.batch.size", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg3));

        Map<String, Object> cfg4 = validConfig();
        cfg4.put("heartbeat.interval.ms", -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg4));

        Map<String, Object> cfg5 = validConfig();
        cfg5.put("incremental.snapshot.chunk.size.mb", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg5));
    }

    @Test
    public void testNumericOptionsOmittedUsesDefaults() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));
    }

    @Test
    public void testSchemaExclusiveConstraints() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));

        Map<String, Object> cfgWithTables = new HashMap<>();
        cfgWithTables.put("hosts", "localhost:27017");
        cfgWithTables.put("database", Collections.singletonList("testdb"));
        cfgWithTables.put("collection", Collections.singletonList("testcol"));
        List<Map<String, Object>> tables = new ArrayList<>();
        tables.add(Collections.singletonMap("table", "db.c1"));
        cfgWithTables.put("tables_configs", tables);
        Assertions.assertDoesNotThrow(() -> validate(cfgWithTables));

        Map<String, Object> cfgBoth = validConfig();
        cfgBoth.put(
                "tables_configs", Collections.singletonList(Collections.singletonMap("t", "v")));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgBoth));

        Map<String, Object> cfgNeither = new HashMap<>();
        cfgNeither.put("hosts", "localhost:27017");
        cfgNeither.put("database", Collections.singletonList("testdb"));
        cfgNeither.put("collection", Collections.singletonList("testcol"));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfgNeither));
    }
}
