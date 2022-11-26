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

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

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
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * This command is used to execute the SeaTunnel engine job by SeaTunnel API.
 */
@Slf4j
public class ClientExecuteCommand implements Command<ClientCommandArgs> {

    private final ClientCommandArgs clientCommandArgs;

    public ClientExecuteCommand(ClientCommandArgs clientCommandArgs) {
        this.clientCommandArgs = clientCommandArgs;
    }

    @SuppressWarnings("checkstyle:RegexpSingleline")
    @Override
    public void execute() throws CommandExecuteException {
        HazelcastInstance instance = null;
        SeaTunnelClient engineClient = null;
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        try {
            String clusterName = clientCommandArgs.getClusterName();
            if (clientCommandArgs.getExecutionMode().equals(ExecutionMode.LOCAL)) {
                clusterName = creatRandomClusterName(clusterName);
                instance = createServerInLocal(clusterName);
            }
            seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(clusterName);
            engineClient = new SeaTunnelClient(clientConfig);
            if (clientCommandArgs.isListJob()) {
                String jobstatus = engineClient.listJobStatus();
                System.out.println(jobstatus);
            } else if (null != clientCommandArgs.getJobId()) {
                String jobState = engineClient.getJobState(Long.parseLong(clientCommandArgs.getJobId()));
                System.out.println(jobState);
            } else {
                Path configFile = FileUtils.getConfigPath(clientCommandArgs);
                checkConfigExist(configFile);
                JobConfig jobConfig = new JobConfig();
                jobConfig.setName(clientCommandArgs.getJobName());
                JobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(configFile.toString(), jobConfig);

                ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
                clientJobProxy.waitForJobComplete();
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new CommandExecuteException("SeaTunnel job executed failed", e);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (instance != null) {
                instance.shutdown();
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
