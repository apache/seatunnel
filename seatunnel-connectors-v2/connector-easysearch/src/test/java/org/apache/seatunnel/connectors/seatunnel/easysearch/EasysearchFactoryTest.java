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

package org.apache.seatunnel.connectors.seatunnel.easysearch;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.easysearch.sink.EasysearchSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.easysearch.source.EasysearchSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EasysearchFactoryTest {

    private final EasysearchSourceFactory sourceFactory = new EasysearchSourceFactory();
    private final EasysearchSinkFactory sinkFactory = new EasysearchSinkFactory();

    private static Map<String, Object> validSourceConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put("hosts", Collections.singletonList("http://127.0.0.1:9200"));
        map.put("index", "demo");
        map.put("source", Collections.singletonList("id"));
        return map;
    }

    private static Map<String, Object> validSinkConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put("hosts", Collections.singletonList("http://127.0.0.1:9200"));
        map.put("index", "demo");
        return map;
    }

    private void validateSource(Map<String, Object> map) {
        ConfigValidator.of(ReadonlyConfig.fromMap(map)).validate(sourceFactory.optionRule());
    }

    private void validateSink(Map<String, Object> map) {
        ConfigValidator.of(ReadonlyConfig.fromMap(map)).validate(sinkFactory.optionRule());
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull(sourceFactory.optionRule());
        Assertions.assertNotNull(sinkFactory.optionRule());
    }

    @Test
    void validSourceConfigPassesValidation() {
        validateSource(validSourceConfig());
    }

    @Test
    void validSinkConfigPassesValidation() {
        validateSink(validSinkConfig());
    }

    @Test
    void validNumericBoundsPassValidation() {
        Map<String, Object> source = validSourceConfig();
        source.put("scroll_size", 1);
        validateSource(source);

        Map<String, Object> sink = validSinkConfig();
        sink.put("max_batch_size", 1);
        sink.put("max_retry_count", 0);
        validateSink(sink);
    }

    @Test
    void blankIndexIsRejectedForSourceAndSink() {
        Map<String, Object> source = validSourceConfig();
        source.put("index", "  ");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(source));

        Map<String, Object> sink = validSinkConfig();
        sink.put("index", "");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(sink));
    }

    @Test
    void emptyHostsIsRejectedForSourceAndSink() {
        Map<String, Object> source = validSourceConfig();
        source.put("hosts", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(source));

        Map<String, Object> sink = validSinkConfig();
        sink.put("hosts", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(sink));
    }

    @Test
    void missingHostsOrIndexIsRejected() {
        Map<String, Object> source = validSourceConfig();
        source.remove("hosts");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(source));

        Map<String, Object> sink = validSinkConfig();
        sink.remove("index");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(sink));
    }

    @Test
    void nonPositiveScrollSizeIsRejected() {
        Map<String, Object> source = validSourceConfig();
        source.put("scroll_size", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(source));
    }

    @Test
    void nonPositiveMaxBatchSizeIsRejected() {
        Map<String, Object> sink = validSinkConfig();
        sink.put("max_batch_size", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(sink));
    }

    @Test
    void negativeMaxRetryCountIsRejected() {
        Map<String, Object> sink = validSinkConfig();
        sink.put("max_retry_count", -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(sink));
    }

    @Test
    void exclusiveSourceAndSchemaRejected() {
        Map<String, Object> map = validSourceConfig();
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", new HashMap<String, Object>());
        map.put("schema", schema);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(map));
    }
}
