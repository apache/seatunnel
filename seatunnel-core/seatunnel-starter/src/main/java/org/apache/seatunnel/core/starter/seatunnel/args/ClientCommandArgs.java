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

package org.apache.seatunnel.core.starter.seatunnel.args;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.command.AbstractCommandArgs;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.command.ConfDecryptCommand;
import org.apache.seatunnel.core.starter.command.ConfEncryptCommand;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.seatunnel.command.ClientExecuteCommand;
import org.apache.seatunnel.core.starter.seatunnel.command.SeaTunnelConfValidateCommand;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class ClientCommandArgs extends AbstractCommandArgs {
    @Parameter(
            names = {"-m", "--master", "-e", "--deploy-mode"},
            description = "SeaTunnel job submit master, support [local, cluster]",
            validateWith = MasterTypeValidator.class,
            converter = SeaTunnelMasterTargetConverter.class)
    private MasterType masterType = MasterType.CLUSTER;

    @Parameter(
            names = {"-r", "--restore"},
            description = "restore with savepoint by jobId")
    private String restoreJobId;

    @Parameter(
            names = {"-s", "--savepoint"},
            description = "savepoint job by jobId")
    private String savePointJobId;

    @Parameter(
            names = {"-cn", "--cluster"},
            description = "The name of cluster")
    private String clusterName;

    @Parameter(
            names = {"-j", "--job-id"},
            description = "Get job status by JobId")
    private String jobId;

    @Parameter(
            names = {"-can", "--cancel-job"},
            variableArity = true,
            description = "Cancel job by JobId")
    private List<String> cancelJobId;

    @Parameter(
            names = {"--metrics"},
            description = "Get job metrics by JobId")
    private String metricsJobId;

    @Parameter(
            names = {"--set-job-id"},
            description = "Set custom job id for job")
    private String customJobId;

    @Parameter(
            names = {"--get_running_job_metrics"},
            description = "Gets metrics for running jobs")
    private boolean getRunningJobMetrics = false;

    @Parameter(
            names = {"-l", "--list"},
            description = "list job status")
    private boolean listJob = false;

    @Parameter(
            names = {"--async"},
            description =
                    "Run the job asynchronously, when the job is submitted, the client will exit")
    private boolean async = false;

    @Parameter(
            names = {"-cj", "--close-job"},
            description = "Close client the task will also be closed")
    private boolean closeJob = true;

    @Override
    public Command<?> buildCommand() {
        Common.setDeployMode(getDeployMode());
        if (checkConfig) {
            return new SeaTunnelConfValidateCommand(this);
        }
        if (encrypt) {
            return new ConfEncryptCommand(this);
        }
        if (decrypt) {
            return new ConfDecryptCommand(this);
        }
        return new ClientExecuteCommand(this);
    }

    public DeployMode getDeployMode() {
        return DeployMode.CLIENT;
    }

    public static class SeaTunnelMasterTargetConverter implements IStringConverter<MasterType> {
        private static final List<MasterType> MASTER_TYPE_LIST = new ArrayList<>();

        static {
            MASTER_TYPE_LIST.add(MasterType.LOCAL);
            MASTER_TYPE_LIST.add(MasterType.CLUSTER);
        }

        @Override
        public MasterType convert(String value) {
            MasterType masterType = MasterType.valueOf(value.toUpperCase());
            if (MASTER_TYPE_LIST.contains(masterType)) {
                return masterType;
            } else {
                throw new IllegalArgumentException(
                        "SeaTunnel job on st-engine submitted target only "
                                + "support these options: [local, cluster]");
            }
        }
    }

    @Slf4j
    public static class MasterTypeValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            if (name.equals("-e") || name.equals("--deploy-mode")) {
                log.warn(
                        "\n******************************************************************************************"
                                + "\n-e and --deploy-mode deprecated in 2.3.1, please use -m and --master instead of it"
                                + "\n******************************************************************************************");
            }
        }
    }
}
