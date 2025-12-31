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

package org.apache.seatunnel.api.configuration.util.issue;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.common.constants.PluginType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DeprecatedConfigurationIssueTest {

    @Test
    public void shouldBuildWarningWithoutSuggestion() {
        Option<String> deprecatedOption =
                new Option<>("deprecated.key", new TypeReference<String>() {}, "old");

        DeprecatedConfigurationIssue issue =
                DeprecatedConfigurationIssue.of(
                        "transform-id", PluginType.TRANSFORM, deprecatedOption);

        Assertions.assertEquals(ConfigurationVerificationIssue.Level.WARNING, issue.getLevel());
        Assertions.assertEquals(
                "Deprecated configuration option 'deprecated.key' detected in transform plugin 'transform-id'",
                issue.getLog());
    }

    @Test
    public void shouldBuildWarningWithSuggestions() {
        Option<String> deprecatedOption =
                new Option<>("deprecated.key", new TypeReference<String>() {}, "old");
        Option<String> referOne =
                new Option<>("suggest.one", new TypeReference<String>() {}, "new-one");
        Option<String> referTwo =
                new Option<>("suggest.two", new TypeReference<String>() {}, "new-two");

        DeprecatedConfigurationIssue issue =
                DeprecatedConfigurationIssue.of(
                        "source-id",
                        PluginType.SOURCE,
                        deprecatedOption,
                        new Option[] {referOne, referTwo});

        Assertions.assertEquals(ConfigurationVerificationIssue.Level.WARNING, issue.getLevel());
        Assertions.assertEquals(
                "Deprecated configuration option 'deprecated.key' detected in source plugin 'source-id', please refer to suggest.one, suggest.two",
                issue.getLog());
    }
}
