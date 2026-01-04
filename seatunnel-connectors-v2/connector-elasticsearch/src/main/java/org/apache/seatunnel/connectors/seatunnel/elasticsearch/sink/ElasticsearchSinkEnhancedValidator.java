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
package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.api.configuration.util.SinkEnhancedConfigurationValidator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.VECTORIZATION_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.VECTOR_DIMENSIONS;

public class ElasticsearchSinkEnhancedValidator extends SinkEnhancedConfigurationValidator {

    public ElasticsearchSinkEnhancedValidator(String identifier) {
        super(identifier);
    }

    @Override
    protected List<VersionCompatibilityRule> versionCompatibilityRules() {
        List<VersionCompatibilityRule> compatibilityRules = new ArrayList<>();
        Predicate<String> isEs73OrAbove =
                version -> {
                    if (version == null || version.isEmpty()) {
                        return false;
                    }
                    try {
                        String[] segments = version.split("\\.");
                        int major = Integer.parseInt(segments[0]);
                        int minor = segments.length > 1 ? Integer.parseInt(segments[1]) : 0;
                        return major > 7 || (major == 7 && minor >= 3);
                    } catch (NumberFormatException e) {
                        return false;
                    }
                };
        compatibilityRules.add(
                VersionCompatibilityRule.warning(VECTORIZATION_FIELDS, isEs73OrAbove, "7.3+"));
        compatibilityRules.add(
                VersionCompatibilityRule.warning(VECTOR_DIMENSIONS, isEs73OrAbove, "7.3+"));
        return compatibilityRules;
    }
}
