/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.source.SupportSchemaEvolution;
import org.apache.seatunnel.translation.flink.schema.SchemaOperator;
import org.apache.seatunnel.translation.flink.schema.SchemaOperator13;

import java.net.URL;
import java.util.List;

/**
 * Flink 1.13-specific source execution processor. Shadows the common {@code SourceExecuteProcessor}
 * at runtime (same package, same class name) to provide two Flink 1.13-specific behaviours without
 * using reflection:
 *
 * <ol>
 *   <li>{@link #createSchemaOperator} returns {@link SchemaOperator13}, which registers the
 *       checkpoint-stall fallback timer via the strongly-typed {@code
 *       ProcessingTimeService.registerTimer} API instead of a background {@code
 *       ScheduledExecutorService} + reflection.
 *   <li>{@link #supportsSinkFunctionFinish} hard-codes {@code false}: Flink 1.13's {@code
 *       SinkFunction} does not expose a {@code finish()} method, so this fact is known at compile
 *       time and no reflection is needed.
 * </ol>
 */
@SuppressWarnings("unchecked,rawtypes")
public class SourceExecuteProcessor extends AbstractSourceExecuteProcessor {

    public SourceExecuteProcessor(
            List<URL> jarPaths,
            Config envConfig,
            List<? extends Config> pluginConfigs,
            JobContext jobContext) {
        super(jarPaths, envConfig, pluginConfigs, jobContext);
    }

    @Override
    protected SchemaOperator createSchemaOperator(
            String jobId, SupportSchemaEvolution source, Config pluginConfig) {
        return new SchemaOperator13(jobId, source, pluginConfig);
    }

    /**
     * Flink 1.13's {@code SinkFunction} does not have a {@code finish()} method, so source
     * keep-alive must always be enabled when schema evolution is active. Returns {@code false}
     * directly rather than using reflection.
     */
    @Override
    protected boolean supportsSinkFunctionFinish() {
        return false;
    }
}
