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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.lineage.LineageConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Binds the two declarations of the lineage option contract to each other.
 *
 * <p>{@code seatunnel-lineage} carries no dependencies so that it can be shaded into a Flink {@code
 * lib} directory, which means it cannot use {@link Option}. Every key and default therefore exists
 * twice: as an {@code Option} that {@code EnvOptionRule} validates a job against, and as a constant
 * that {@link LineageConfig} resolves the running job with. Nothing links them at compile time, so
 * a change to one silently produces a job that is validated against one default and runs with
 * another. These assertions are that link.
 */
class LineageOptionParityTest {

    @Test
    void optionKeysMatchTheResolverConstants() {
        assertKey(EnvCommonOptions.OPENLINEAGE_ENABLED, LineageConfig.ENABLED);
        assertKey(EnvCommonOptions.OPENLINEAGE_TRANSPORT, LineageConfig.TRANSPORT);
        assertKey(EnvCommonOptions.OPENLINEAGE_URL, LineageConfig.URL);
        assertKey(EnvCommonOptions.OPENLINEAGE_NAMESPACE, LineageConfig.NAMESPACE);
        assertKey(EnvCommonOptions.OPENLINEAGE_AUTH_TOKEN, LineageConfig.AUTH_TOKEN);
        assertKey(EnvCommonOptions.OPENLINEAGE_TIMEOUT_MS, LineageConfig.TIMEOUT_MS);
        assertKey(EnvCommonOptions.OPENLINEAGE_RETRY_TIMES, LineageConfig.RETRY_TIMES);
        assertKey(EnvCommonOptions.OPENLINEAGE_RUN_FACET, LineageConfig.RUN_FACET);
        assertKey(EnvCommonOptions.OPENLINEAGE_RUN_PROPERTIES, LineageConfig.RUN_PROPERTIES);
        assertKey(
                EnvCommonOptions.OPENLINEAGE_HEARTBEAT_MIN_INTERVAL_MS,
                LineageConfig.HEARTBEAT_MIN_INTERVAL_MS);
        assertKey(EnvCommonOptions.OPENLINEAGE_PRODUCER, LineageConfig.PRODUCER);
    }

    @Test
    void optionDefaultsMatchTheResolverDefaults() {
        LineageConfig defaults = LineageConfig.defaults();

        Assertions.assertEquals(
                EnvCommonOptions.OPENLINEAGE_ENABLED.defaultValue(), defaults.enabled());
        Assertions.assertEquals(
                EnvCommonOptions.OPENLINEAGE_TRANSPORT.defaultValue(),
                LineageConfig.DEFAULT_TRANSPORT);
        Assertions.assertEquals(
                EnvCommonOptions.OPENLINEAGE_NAMESPACE.defaultValue(),
                LineageConfig.DEFAULT_NAMESPACE);
        Assertions.assertEquals(
                EnvCommonOptions.OPENLINEAGE_RUN_FACET.defaultValue(),
                LineageConfig.DEFAULT_RUN_FACET);
        Assertions.assertEquals(
                EnvCommonOptions.OPENLINEAGE_TIMEOUT_MS.defaultValue().intValue(),
                LineageConfig.DEFAULT_TIMEOUT_MS);
        Assertions.assertEquals(
                EnvCommonOptions.OPENLINEAGE_RETRY_TIMES.defaultValue().intValue(),
                LineageConfig.DEFAULT_RETRY_TIMES);
        Assertions.assertEquals(
                EnvCommonOptions.OPENLINEAGE_HEARTBEAT_MIN_INTERVAL_MS.defaultValue().longValue(),
                LineageConfig.DEFAULT_HEARTBEAT_MIN_INTERVAL_MS);
    }

    private static void assertKey(Option<?> option, String resolverKey) {
        Assertions.assertEquals(
                option.key(),
                resolverKey,
                "the declared option and the resolver constant must name the same setting");
    }
}
