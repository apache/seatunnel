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

package org.apache.seatunnel.connectors.seatunnel.hudi.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class HudiSinkConfigTest {

    @Test
    void shouldUseTheAtLeastOnceSemanticsByDefault() {
        HudiSinkConfig sinkConfig = HudiSinkConfig.of(config(null));

        Assertions.assertFalse(sinkConfig.isExactlyOnce());
        Assertions.assertEquals(HudiSemantics.AT_LEAST_ONCE, sinkConfig.getSemantics());
    }

    @Test
    void shouldUseTheExactlyOnceSemanticsWhenItIsConfigured() {
        HudiSinkConfig sinkConfig = HudiSinkConfig.of(config(HudiSemantics.EXACTLY_ONCE));

        Assertions.assertTrue(sinkConfig.isExactlyOnce());
        Assertions.assertEquals(HudiSemantics.EXACTLY_ONCE, sinkConfig.getSemantics());
    }

    @Test
    void shouldFailWhenTheSemanticsIsUnknown() {
        Map<String, Object> options = baseOptions();
        options.put(HudiSinkOptions.SEMANTICS.key(), "EXACTLY_TWICE");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> HudiSinkConfig.of(ReadonlyConfig.fromMap(options)));
    }

    private ReadonlyConfig config(HudiSemantics semantics) {
        Map<String, Object> options = baseOptions();
        if (semantics != null) {
            options.put(HudiSinkOptions.SEMANTICS.key(), semantics.name());
        }
        return ReadonlyConfig.fromMap(options);
    }

    private Map<String, Object> baseOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(HudiSinkOptions.TABLE_DFS_PATH.key(), "/tmp/hudi");
        options.put(HudiSinkOptions.TABLE_NAME.key(), "st_test");
        return options;
    }
}
