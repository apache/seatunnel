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

package org.apache.seatunnel.connectors.seatunnel.deeplake.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.connectors.seatunnel.deeplake.config.DeepLakeSinkOptions;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DeepLakeSinkFactoryTest {

    private static final DeepLakeSinkFactory FACTORY = new DeepLakeSinkFactory();

    @Test
    void exposesExpectedIdentifierAndAcceptsValidOptions() {
        assertEquals("DeepLake", FACTORY.factoryIdentifier());
        assertDoesNotThrow(() -> validate(requiredOptions()));
    }

    @Test
    void rejectsMissingRequiredOptions() {
        for (String option : new String[] {"api_key", "org_id", "workspace"}) {
            Map<String, Object> config = requiredOptions();
            config.remove(option);
            assertThrows(OptionValidationException.class, () -> validate(config));
        }
    }

    @Test
    void rejectsInvalidOptionalValues() {
        Map<String, Object> blankApiUrl = requiredOptions();
        blankApiUrl.put(DeepLakeSinkOptions.API_URL.key(), " ");
        assertThrows(OptionValidationException.class, () -> validate(blankApiUrl));

        Map<String, Object> zeroBatchSize = requiredOptions();
        zeroBatchSize.put(DeepLakeSinkOptions.BATCH_SIZE.key(), 0);
        assertThrows(OptionValidationException.class, () -> validate(zeroBatchSize));

        Map<String, Object> recreateSchema = requiredOptions();
        recreateSchema.put(
                DeepLakeSinkOptions.SCHEMA_SAVE_MODE.key(), SchemaSaveMode.RECREATE_SCHEMA.name());
        assertThrows(OptionValidationException.class, () -> validate(recreateSchema));
    }

    private static Map<String, Object> requiredOptions() {
        Map<String, Object> config = new HashMap<>();
        config.put(DeepLakeSinkOptions.API_KEY.key(), "test-api-key");
        config.put(DeepLakeSinkOptions.ORG_ID.key(), "test-org");
        config.put(DeepLakeSinkOptions.WORKSPACE.key(), "research");
        return config;
    }

    private static void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(FACTORY.optionRule());
    }
}
