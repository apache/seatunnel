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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Reusable {@link ConditionExtension} validators shared by Elasticsearch Source / Sink / Catalog
 * factories.
 *
 * <p>All validators return {@code false} on failure (instead of throwing) so that {@code
 * ConfigValidator} can aggregate errors across the entire rule.
 */
@UtilityClass
public final class ElasticsearchValidators {

    /**
     * Validates that {@code auth.api_key_encoded} is a Base64-encoded {@code id:key} string.
     *
     * <p>Skips when the value is null/blank — presence and non-blankness are enforced separately by
     * the corresponding conditional rules.
     */
    @Slf4j
    public static class ApiKeyEncodedFormatValidator implements ConditionExtension<String> {
        @Override
        public String description() {
            return "'auth.api_key_encoded' must be a Base64-encoded 'id:key' string";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String value) {
            if (value == null || value.trim().isEmpty()) {
                return true;
            }
            try {
                byte[] decoded = Base64.getDecoder().decode(value);
                return new String(decoded, StandardCharsets.UTF_8).contains(":");
            } catch (IllegalArgumentException e) {
                log.warn("Failed to decode 'auth.api_key_encoded' as Base64", e);
                return false;
            }
        }
    }
}
