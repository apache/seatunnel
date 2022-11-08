/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.apache.seatunnel.core.starter.spark.execution;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.spark.SparkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class TransformV2ExecuteProcessor extends AbstractPluginExecuteProcessor<SeaTunnelTransform<?>> {

    private static final String PLUGIN_TYPE = "transform";

    protected TransformV2ExecuteProcessor(SparkEnvironment sparkEnvironment, JobContext jobContext, List<? extends Config> pluginConfigs) {
        super(sparkEnvironment, jobContext, pluginConfigs);
    }

    @Override
    protected List<SeaTunnelTransform<?>> initializePlugins(List<? extends Config> pluginConfigs) {
        return null;
    }

    @Override
    public List<Dataset<Row>> execute(List<Dataset<Row>> upstreamDataStreams) throws TaskExecuteException {
        return null;
    }
}
