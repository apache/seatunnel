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

import java.util.Optional;

public class VersionCompatibilityConfigurationIssueTest {

    @Test
    void shouldBuildErrorIssueWithKnownCurrentVersion() {
        Option<String> option =
                new Option<>("compatibility.key", new TypeReference<String>() {}, "default");

        VersionCompatibilityConfigurationIssue issue =
                VersionCompatibilityConfigurationIssue.errorOf(
                        "sink-id", PluginType.SINK, option, "2.0", Optional.of("1.5"));

        Assertions.assertEquals(ConfigurationVerificationIssue.Level.ERROR, issue.getLevel());
        Assertions.assertEquals(
                "[SeaTunnel Config Validation] Configuration option 'compatibility.key' requires version '2.0' (current version '1.5') in sink plugin 'sink-id'",
                issue.getLog());
    }

    @Test
    void shouldBuildWarningIssueWithUnknownCurrentVersion() {
        Option<String> option =
                new Option<>("compatibility.key", new TypeReference<String>() {}, "default");

        VersionCompatibilityConfigurationIssue issue =
                VersionCompatibilityConfigurationIssue.warnOf(
                        "source-id", PluginType.SOURCE, option, "3.1", Optional.empty());

        Assertions.assertEquals(ConfigurationVerificationIssue.Level.WARNING, issue.getLevel());
        Assertions.assertEquals(
                "[SeaTunnel Config Validation] Configuration option 'compatibility.key' requires version '3.1' (current version is unknown) in source plugin 'source-id'",
                issue.getLog());
    }
}
