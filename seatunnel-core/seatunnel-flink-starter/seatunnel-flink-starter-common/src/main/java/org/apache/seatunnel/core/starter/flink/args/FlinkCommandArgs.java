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

package org.apache.seatunnel.core.starter.flink.args;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.command.AbstractCommandArgs;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.command.ConfDecryptCommand;
import org.apache.seatunnel.core.starter.command.ConfEncryptCommand;
import org.apache.seatunnel.core.starter.enums.DiscoveryType;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.flink.command.FlinkConfValidateCommand;
import org.apache.seatunnel.core.starter.flink.command.FlinkTaskExecuteCommand;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class FlinkCommandArgs extends AbstractCommandArgs {

    @Parameter(
            names = {"-e", "--deploy-mode"},
            converter = FlinkDeployModeConverter.class,
            description = "Flink job deploy mode, support [run, run-application]")
    private DeployMode deployMode = DeployMode.RUN;

    @Parameter(
            names = {"--master", "--target"},
            converter = FlinkMasterTargetConverter.class,
            description =
                    "Flink job submitted target master, support [local, remote, yarn-session, yarn-per-job, "
                            + "kubernetes-session, yarn-application, kubernetes-application]")
    private MasterType masterType;

    @Parameter(
            names = {"-s", "--connectors"},
            description =
                    "A semicolon-separated list of the classpaths to package with the job jars to be sent to the cluster. These have to be valid URLs.")
    private String connectors = "";

    @Parameter(
            names = {"-y", "--discovery"},
            converter = FlinkPluginDiscoverTypeConverter.class,
            description = "SeaTunnel discovery extend plugin library type.")
    private DiscoveryType discoveryType = DiscoveryType.LOCAL;

    @Override
    public Command<?> buildCommand() {
        Common.setDeployMode(getDeployMode());
        if (checkConfig) {
            return new FlinkConfValidateCommand(this);
        }
        if (encrypt) {
            return new ConfEncryptCommand(this);
        }
        if (decrypt) {
            return new ConfDecryptCommand(this);
        }
        return new FlinkTaskExecuteCommand(this);
    }

    @Override
    public String toString() {
        return "FlinkCommandArgs{"
                + "deployMode="
                + deployMode
                + ", masterType="
                + masterType
                + ", configFile='"
                + configFile
                + '\''
                + ", variables="
                + variables
                + ", jobName='"
                + jobName
                + '\''
                + ", originalParameters="
                + originalParameters
                + '}';
    }

    public static class FlinkMasterTargetConverter implements IStringConverter<MasterType> {
        private static final List<MasterType> MASTER_TYPE_LIST = new ArrayList<>();

        static {
            MASTER_TYPE_LIST.add(MasterType.LOCAL);
            MASTER_TYPE_LIST.add(MasterType.REMOTE);
            MASTER_TYPE_LIST.add(MasterType.YARN_SESSION);
            MASTER_TYPE_LIST.add(MasterType.YARN_PER_JOB);
            MASTER_TYPE_LIST.add(MasterType.KUBERNETES_SESSION);
            MASTER_TYPE_LIST.add(MasterType.YARN_APPLICATION);
            MASTER_TYPE_LIST.add(MasterType.KUBERNETES_APPLICATION);
        }

        @Override
        public MasterType convert(String value) {
            MasterType masterType = MasterType.valueOf(value.toUpperCase().replaceAll("-", "_"));
            if (MASTER_TYPE_LIST.contains(masterType)) {
                return masterType;
            } else {
                throw new IllegalArgumentException(
                        "SeaTunnel job on flink engine submitted target only "
                                + "support these options: [local, remote, yarn-session, yarn-per-job, kubernetes-session, "
                                + "yarn-application, kubernetes-application]");
            }
        }
    }

    public static class FlinkDeployModeConverter implements IStringConverter<DeployMode> {
        private static final List<DeployMode> DEPLOY_MODE_TYPE_LIST = new ArrayList<>();

        static {
            DEPLOY_MODE_TYPE_LIST.add(DeployMode.RUN);
            DEPLOY_MODE_TYPE_LIST.add(DeployMode.RUN_APPLICATION);
        }

        @Override
        public DeployMode convert(String value) {
            DeployMode deployMode = DeployMode.valueOf(value.toUpperCase().replaceAll("-", "_"));
            if (DEPLOY_MODE_TYPE_LIST.contains(deployMode)) {
                return deployMode;
            } else {
                throw new IllegalArgumentException(
                        "SeaTunnel job on flink engine deploy mode only "
                                + "support these options: [run, run-application]");
            }
        }
    }

    public static class FlinkPluginDiscoverTypeConverter
            implements IStringConverter<DiscoveryType> {
        private static final List<DiscoveryType> DISCOVER_TYPE_LIST = new ArrayList<>();

        static {
            DISCOVER_TYPE_LIST.add(DiscoveryType.LOCAL);
            DISCOVER_TYPE_LIST.add(DiscoveryType.REMOTE);
        }

        @Override
        public DiscoveryType convert(String value) {
            DiscoveryType discoveryType = DiscoveryType.valueOf(value.toUpperCase());
            if (DISCOVER_TYPE_LIST.contains(discoveryType)) {
                return discoveryType;
            } else {
                throw new IllegalArgumentException(
                        "SeaTunnel job load plugin type only "
                                + "support these options: [local, remote]");
            }
        }
    }
}
