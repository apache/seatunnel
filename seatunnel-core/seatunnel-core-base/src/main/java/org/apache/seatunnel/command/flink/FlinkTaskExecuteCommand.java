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

package org.apache.seatunnel.command.flink;

import org.apache.seatunnel.apis.BaseSink;
import org.apache.seatunnel.apis.BaseSource;
import org.apache.seatunnel.apis.BaseTransform;
import org.apache.seatunnel.command.BaseTaskExecuteCommand;
import org.apache.seatunnel.command.FlinkCommandArgs;
import org.apache.seatunnel.config.ConfigBuilder;
import org.apache.seatunnel.config.PluginType;
import org.apache.seatunnel.env.Execution;

import java.util.List;

/**
 * Used to execute Flink Job.
 */
public class FlinkTaskExecuteCommand extends BaseTaskExecuteCommand<FlinkCommandArgs> {

    @Override
    public void execute(FlinkCommandArgs flinkCommandArgs) {
        ConfigBuilder configBuilder = new ConfigBuilder(flinkCommandArgs.getConfigFile(), flinkCommandArgs.getEngineType());

        List<BaseSource> sources = configBuilder.createPlugins(PluginType.SOURCE);
        List<BaseTransform> transforms = configBuilder.createPlugins(PluginType.TRANSFORM);
        List<BaseSink> sinks = configBuilder.createPlugins(PluginType.SINK);
        Execution execution = configBuilder.createExecution();
        baseCheckConfig(sources, transforms, sinks);
        prepare(configBuilder.getEnv(), sources, transforms, sinks);
        showAsciiLogo();

        try {
            execution.start(sources, transforms, sinks);
        } catch (Exception e) {
            throw new RuntimeException("Execute Flink task error", e);
        }
    }

}
