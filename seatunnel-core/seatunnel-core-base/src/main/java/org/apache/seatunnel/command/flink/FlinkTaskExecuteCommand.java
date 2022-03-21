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
import org.apache.seatunnel.flink.FlinkEnvironment;

import java.util.List;

/**
 * Used to execute Flink Job.
 */
public class FlinkTaskExecuteCommand extends BaseTaskExecuteCommand<FlinkCommandArgs, FlinkEnvironment> {

    @Override
    public void execute(FlinkCommandArgs flinkCommandArgs) {
        ConfigBuilder<FlinkEnvironment> configBuilder = new ConfigBuilder<>(flinkCommandArgs.getConfigFile(), flinkCommandArgs.getEngineType());

        List<BaseSource<FlinkEnvironment>> sources = configBuilder.createPlugins(PluginType.SOURCE);
        List<BaseTransform<FlinkEnvironment>> transforms = configBuilder.createPlugins(PluginType.TRANSFORM);
        List<BaseSink<FlinkEnvironment>> sinks = configBuilder.createPlugins(PluginType.SINK);

        Execution<BaseSource<FlinkEnvironment>, BaseTransform<FlinkEnvironment>, BaseSink<FlinkEnvironment>, FlinkEnvironment>
            execution = configBuilder.createExecution();

        baseCheckConfig(sources, transforms, sinks);
        showAsciiLogo();

        try {
            open(configBuilder.getEnv(), sources, transforms, sinks);
            execution.start(sources, transforms, sinks);
            close(sources, transforms, sinks);
        } catch (Exception e) {
            throw new RuntimeException("Execute Flink task error", e);
        }
    }

}
