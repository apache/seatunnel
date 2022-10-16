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
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
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
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * This command is used to execute the SeaTunnel engine job by SeaTunnel API.
 */
public class ClientExecuteCommand implements Command<ClientCommandArgs> {

    private final ClientCommandArgs clientCommandArgs;

    public ClientExecuteCommand(ClientCommandArgs clientCommandArgs) {
        this.clientCommandArgs = clientCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        Path configFile = FileUtils.getConfigPath(clientCommandArgs);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(clientCommandArgs.getJobName());
        HazelcastInstance instance = null;
        ClientJobProxy clientJobProxy = null;
        try {
            String clusterName = clientCommandArgs.getClusterName();
            if (clientCommandArgs.getExecutionMode().equals(ExecutionMode.LOCAL)) {
                clusterName = creatRandomClusterName(clusterName);
                instance = createServerInLocal(clusterName);
            }
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(clusterName);
            SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
            JobExecutionEnvironment jobExecutionEnv = engineClient.createExecutionContext(configFile.toString(), jobConfig);

            clientJobProxy = jobExecutionEnv.execute();
            clientJobProxy.waitForJobComplete();
        } catch (ExecutionException | InterruptedException e) {
            throw new CommandExecuteException("SeaTunnel job executed failed", e);
        } finally {
            if (instance != null) {
                instance.shutdown();
            }
            if (clientJobProxy != null) {
                clientJobProxy.close();
            }
        }
    }

    private HazelcastInstance createServerInLocal(String clusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        return HazelcastInstanceFactory.newHazelcastInstance(seaTunnelConfig.getHazelcastConfig(),
            Thread.currentThread().getName(),
            new SeaTunnelNodeContext(seaTunnelConfig));
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private String creatRandomClusterName(String namePrefix) {
        Random random = new Random();
        return namePrefix + "-" + random.nextInt(1000000);
    }

}
