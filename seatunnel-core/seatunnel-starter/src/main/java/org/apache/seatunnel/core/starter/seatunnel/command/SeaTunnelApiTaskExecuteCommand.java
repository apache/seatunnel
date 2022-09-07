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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.seatunnel.args.SeaTunnelCommandArgs;
import org.apache.seatunnel.core.starter.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

/**
 * This command is used to execute the SeaTunnel engine job by SeaTunnel API.
 */
public class SeaTunnelApiTaskExecuteCommand implements Command<SeaTunnelCommandArgs> {

    private final SeaTunnelCommandArgs seaTunnelCommandArgs;

    // TODO custom cluster name on cluster execution mode
    private static final String CLUSTER_NAME = "SeaTunnelCluster";

    public SeaTunnelApiTaskExecuteCommand(SeaTunnelCommandArgs seaTunnelCommandArgs) {
        this.seaTunnelCommandArgs = seaTunnelCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        Path configFile = FileUtils.getConfigPath(seaTunnelCommandArgs);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(seaTunnelCommandArgs.getName());

        HazelcastInstance instance = null;
        if (seaTunnelCommandArgs.getExecutionMode().equals(ExecutionMode.LOCAL)) {
            instance = createServerInLocal();
        }

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(CLUSTER_NAME);
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        JobExecutionEnvironment jobExecutionEnv = engineClient.createExecutionContext(configFile.toString(), jobConfig);

        ClientJobProxy clientJobProxy;
        try {
            clientJobProxy = jobExecutionEnv.execute();
            clientJobProxy.waitForJobComplete();
        } catch (ExecutionException | InterruptedException e) {
            throw new CommandExecuteException("SeaTunnel job executed failed", e);
        } finally {
            if (instance != null) {
                instance.shutdown();
            }
        }
    }

    private HazelcastInstance createServerInLocal() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(CLUSTER_NAME);
        return HazelcastInstanceFactory.newHazelcastInstance(seaTunnelConfig.getHazelcastConfig(),
            Thread.currentThread().getName(),
            new SeaTunnelNodeContext(ConfigProvider.locateAndGetSeaTunnelConfig()));
    }

}
