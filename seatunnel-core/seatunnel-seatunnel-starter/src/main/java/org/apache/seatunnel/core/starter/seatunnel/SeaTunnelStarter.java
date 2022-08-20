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

package org.apache.seatunnel.core.starter.seatunnel;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.seatunnel.args.SeaTunnelCommandArgs;
import org.apache.seatunnel.core.starter.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.JobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;

import com.hazelcast.client.config.ClientConfig;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

public class SeaTunnelStarter {
    public static void main(String[] args) {
        SeaTunnelCommandArgs seaTunnelCommandArgs = CommandLineUtils.parseSeaTunnelArgs(args);
        Path configFile = FileUtils.getConfigPath(seaTunnelCommandArgs);
        Common.setDeployMode(DeployMode.CLIENT);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");
        // TODO change jobConfig mode

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        JobExecutionEnvironment jobExecutionEnv = engineClient.createExecutionContext(configFile.toString(), jobConfig);

        JobProxy jobProxy;
        try {
            jobProxy = jobExecutionEnv.execute();
            jobProxy.waitForJobComplete();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
