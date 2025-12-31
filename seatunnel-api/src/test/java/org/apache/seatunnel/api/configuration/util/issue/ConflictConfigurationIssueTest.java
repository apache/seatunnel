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

public class ConflictConfigurationIssueTest {

    @Test
    public void shouldBuildErrorIssueWithExpectedMessage() {
        Option<String> option =
                new Option<>("first.key", new TypeReference<String>() {}, "first-value");
        Option<String> conflictOption =
                new Option<>("conflict.key", new TypeReference<String>() {}, "conflict-value");

        ConflictConfigurationIssue issue =
                ConflictConfigurationIssue.errorOf(
                        "test-identifier",
                        PluginType.SOURCE,
                        option,
                        "provided-first",
                        conflictOption,
                        "provided-conflict");

        Assertions.assertEquals(ConfigurationVerificationIssue.Level.ERROR, issue.getLevel());
        Assertions.assertEquals("test-identifier", issue.getIdentifier());
        Assertions.assertEquals(PluginType.SOURCE, issue.getPluginType());
        Assertions.assertEquals(
                "Configuration option 'first.key' with value 'provided-first' conflicts with option 'conflict.key' (value 'provided-conflict') in source plugin 'test-identifier'",
                issue.getLog());
    }

    @Test
    public void shouldBuildWarningIssueWithExpectedMessage() {
        Option<String> option =
                new Option<>("first.key", new TypeReference<String>() {}, "first-value");
        Option<String> conflictOption =
                new Option<>("conflict.key", new TypeReference<String>() {}, "conflict-value");

        ConflictConfigurationIssue issue =
                ConflictConfigurationIssue.warnOf(
                        "test-sink", PluginType.SINK, option, "value-a", conflictOption, "value-b");

        Assertions.assertEquals(ConfigurationVerificationIssue.Level.WARNING, issue.getLevel());
        Assertions.assertEquals(
                "Configuration option 'first.key' with value 'value-a' conflicts with option 'conflict.key' (value 'value-b') in sink plugin 'test-sink'",
                issue.getLog());
    }
}
