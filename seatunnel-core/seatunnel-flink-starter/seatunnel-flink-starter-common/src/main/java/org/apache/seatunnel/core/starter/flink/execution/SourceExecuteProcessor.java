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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;

import java.net.URL;
import java.util.List;

/**
 * Default (Flink 1.15+) source execution processor. Delegates entirely to {@link
 * AbstractSourceExecuteProcessor}. For Flink 1.13, this class is shadowed at runtime by the version
 * in {@code seatunnel-flink-13-starter}, which overrides {@link #createSchemaOperator} and {@link
 * #supportsSinkFunctionFinish} with strongly-typed Flink 1.13 implementations that avoid
 * reflection.
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
}
