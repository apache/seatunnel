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

package org.apache.seatunnel.core.starter.validation;

import org.apache.seatunnel.common.utils.JsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConfigValidationResultTest {

    @Test
    void serializesSuccessWithStableSchema() throws Exception {
        ConfigValidationResult result = ConfigValidationResult.success("static");

        Assertions.assertEquals(
                "{\"schemaVersion\":\"1.0\",\"valid\":true,"
                        + "\"phase\":\"static\",\"errors\":[]}",
                result.toJson());
        Assertions.assertTrue(JsonUtils.readTree(result.toJson()).get("valid").asBoolean());
        Assertions.assertEquals("VALID", result.toHumanReadable());
    }

    @Test
    void serializesFailureAndPreservesNullableFields() throws Exception {
        ConfigValidationError error =
                new ConfigValidationError(
                        "source[0](Kafka)", "Kafka", null, "option", "Required option is missing");
        ConfigValidationResult result =
                ConfigValidationResult.failure("static", error);

        Assertions.assertEquals(
                "{\"schemaVersion\":\"1.0\",\"valid\":false,"
                        + "\"phase\":\"static\",\"errors\":[{"
                        + "\"location\":\"source[0](Kafka)\",\"plugin\":\"Kafka\","
                        + "\"optionPath\":null,\"ruleCategory\":\"option\","
                        + "\"message\":\"Required option is missing\"}]}",
                result.toJson());
        Assertions.assertEquals(
                "Static analysis failed: Required option is missing", result.toHumanReadable());
        Assertions.assertEquals(
                "option", JsonUtils.readTree(result.toJson()).at("/errors/0/ruleCategory").asText());
    }
}
