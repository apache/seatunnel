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

package org.apache.seatunnel.core.starter.spark.args;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.command.AbstractCommandArgs;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.command.ConfDecryptCommand;
import org.apache.seatunnel.core.starter.command.ConfEncryptCommand;
import org.apache.seatunnel.core.starter.spark.command.SparkConfValidateCommand;
import org.apache.seatunnel.core.starter.spark.command.SparkTaskExecuteCommand;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class SparkCommandArgs extends AbstractCommandArgs {

    @Parameter(
            names = {"-e", "--deploy-mode"},
            description = "Spark deploy mode, support [cluster, client]",
            converter = SparkDeployModeConverter.class)
    private DeployMode deployMode = DeployMode.CLIENT;

    @Parameter(
            names = {"-m", "--master"},
            description =
                    "Spark master, support [spark://host:port, mesos://host:port, yarn, "
                            + "k8s://https://host:port, local], default local[*]")
    private String master = "local[*]";

    @Override
    public Command<?> buildCommand() {
        Common.setDeployMode(getDeployMode());
        if (checkConfig) {
            return new SparkConfValidateCommand(this);
        }
        if (encrypt) {
            return new ConfEncryptCommand(this);
        }
        if (decrypt) {
            return new ConfDecryptCommand(this);
        }
        return new SparkTaskExecuteCommand(this);
    }

    public static class SparkDeployModeConverter implements IStringConverter<DeployMode> {
        private static final List<DeployMode> DEPLOY_MODE_TYPE_LIST = new ArrayList<>();

        static {
            DEPLOY_MODE_TYPE_LIST.add(DeployMode.CLIENT);
            DEPLOY_MODE_TYPE_LIST.add(DeployMode.CLUSTER);
        }

        @Override
        public DeployMode convert(String value) {
            DeployMode deployMode = DeployMode.valueOf(value.toUpperCase());
            if (DEPLOY_MODE_TYPE_LIST.contains(deployMode)) {
                return deployMode;
            } else {
                throw new IllegalArgumentException(
                        "SeaTunnel job on spark engine deploy mode only "
                                + "support these options: [cluster, client]");
            }
        }
    }
}
