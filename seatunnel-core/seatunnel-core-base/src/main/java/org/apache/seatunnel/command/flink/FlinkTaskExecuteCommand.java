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
import org.apache.seatunnel.config.EngineType;
import org.apache.seatunnel.config.ExecutionContext;
import org.apache.seatunnel.config.ExecutionFactory;
import org.apache.seatunnel.env.Execution;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.utils.FileUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.nio.file.Path;
import java.util.List;

/**
 * Used to execute Flink Job.
 */
public class FlinkTaskExecuteCommand extends BaseTaskExecuteCommand<FlinkCommandArgs, FlinkEnvironment> {

    @Override
    public void execute(FlinkCommandArgs flinkCommandArgs) {
        EngineType engine = flinkCommandArgs.getEngineType();
        Path configFile = FileUtils.getConfigPath(flinkCommandArgs);

        Config config = new ConfigBuilder<>(configFile, engine).getConfig();
        ExecutionContext<FlinkEnvironment> executionContext = new ExecutionContext<>(config, engine);
        List<BaseSource<FlinkEnvironment>> sources = executionContext.getSources();
        List<BaseTransform<FlinkEnvironment>> transforms = executionContext.getTransforms();
        List<BaseSink<FlinkEnvironment>> sinks = executionContext.getSinks();

        baseCheckConfig(sinks, transforms, sinks);
        showAsciiLogo();

        try (Execution<BaseSource<FlinkEnvironment>,
                BaseTransform<FlinkEnvironment>,
                BaseSink<FlinkEnvironment>,
                FlinkEnvironment> execution = new ExecutionFactory<>(executionContext).createExecution()) {
            prepare(executionContext.getEnvironment(), sources, transforms, sinks);
            execution.start(sources, transforms, sinks);
            close(sources, transforms, sinks);
        } catch (Exception e) {
            throw new RuntimeException("Execute Flink task error", e);
        }
    }

}
