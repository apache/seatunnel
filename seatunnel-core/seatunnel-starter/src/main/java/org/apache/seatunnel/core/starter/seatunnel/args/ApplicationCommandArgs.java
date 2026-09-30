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

import org.apache.seatunnel.common.constants.ApplicationOperation;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.command.CommandArgs;
import org.apache.seatunnel.core.starter.seatunnel.command.ApplicationExecuteCommand;
import org.apache.seatunnel.engine.common.runtime.DeployType;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
@Parameters(separators = "=")
public class ApplicationCommandArgs extends CommandArgs {

    @Parameter(
            description = "submit | status | cancel",
            required = true,
            converter = ApplicationOperationConverter.class)
    private ApplicationOperation operation;

    @Parameter(
            names = {"-t", "--target"},
            description = "External resource platform: yarn or kubernetes",
            required = true,
            converter = DeployTypeConverter.class)
    private DeployType target;

    @Parameter(
            names = {"-c", "--config"},
            description = "SeaTunnel job configuration file (required for submit only)")
    private String config;

    @Parameter(names = "--job-id", description = "Optional native Zeta job ID for submit")
    private Long jobId;

    @Parameter(
            names = "--restore-job-id",
            description =
                    "Historical Zeta job ID whose latest checkpoint restores this new application")
    private Long restoreJobId;

    @Parameter(
            names = {"-a", "--application-config"},
            description =
                    "Optional HOCON deployment file, e.g. application.config (not the job file)")
    private String applicationConfig;

    @Parameter(names = "--id", description = "Platform application ID (required for status/cancel)")
    private String id;

    @Parameter(
            names = "--wait",
            description = "Wait for completion (submit/status only; default: return immediately)")
    private boolean wait;

    @DynamicParameter(
            names = "-i",
            description = "Override a non-sensitive deployment option: -ikey=value")
    private Map<String, String> options = new LinkedHashMap<>();

    @Override
    public Command<?> buildCommand() {
        validateCommandOptions();
        return new ApplicationExecuteCommand(this);
    }

    /**
     * Validates command-specific arguments before reading files or opening platform connections.
     */
    public void validateCommandOptions() {
        if (applicationConfig != null) {
            requireFile(applicationConfig, "--application-config");
        }
        if (operation == ApplicationOperation.SUBMIT) {
            if (config == null) {
                throw new IllegalArgumentException("submit requires --config");
            }
            requireFile(config, "--config");
            if (id != null) {
                throw new IllegalArgumentException("submit does not accept --id");
            }
            return;
        }
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("status/cancel require --id");
        }
        if (config != null) {
            throw new IllegalArgumentException("status/cancel do not accept --config");
        }
        if (jobId != null || restoreJobId != null) {
            throw new IllegalArgumentException(
                    "--job-id and --restore-job-id are supported only with submit");
        }
        if (wait && operation == ApplicationOperation.CANCEL) {
            throw new IllegalArgumentException("--wait is supported only with submit or status");
        }
    }

    private static void requireFile(String path, String option) {
        if (!Files.isRegularFile(Paths.get(path))) {
            throw new IllegalArgumentException(option + " file does not exist: " + path);
        }
    }

    public static class ApplicationOperationConverter
            implements IStringConverter<ApplicationOperation> {

        @Override
        public ApplicationOperation convert(String value) {
            if (value != null) {
                for (ApplicationOperation operation : ApplicationOperation.values()) {
                    if (operation.getOperation().equalsIgnoreCase(value.trim())) {
                        return operation;
                    }
                }
            }
            throw new ParameterException(
                    "Unsupported operation '"
                            + value
                            + "'. Currently only [submit, cancel, status] are supported!");
        }
    }

    public static class DeployTypeConverter implements IStringConverter<DeployType> {

        @Override
        public DeployType convert(String value) {
            if (value != null && DeployType.YARN.name().equalsIgnoreCase(value.trim())) {
                return DeployType.YARN;
            }
            if (value != null && DeployType.KUBERNETES.name().equalsIgnoreCase(value.trim())) {
                return DeployType.KUBERNETES;
            }
            if (value != null && DeployType.STANDALONE.name().equalsIgnoreCase(value.trim())) {
                throw new ParameterException(
                        "Use seatunnel.sh for standalone jobs; application targets are yarn and kubernetes");
            }
            throw new ParameterException(
                    "Unsupported target '"
                            + value
                            + "'. Currently only [yarn, kubernetes] are supported!");
        }
    }
}
