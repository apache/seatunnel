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

    /** Default status poll interval while {@code --wait} blocks the client. */
    public static final long DEFAULT_WAIT_INTERVAL_MILLIS = 500L;

    @Parameter(
            description = "submit | status | cancel",
            required = true,
            converter = ApplicationOperationConverter.class)
    private ApplicationOperation operation;

    @Parameter(
            names = {"-t", "--target"},
            description = "External resource platform: kubernetes",
            required = true,
            converter = DeployTypeConverter.class)
    private DeployType target;

    @Parameter(
            names = {"-c", "--config"},
            description =
                    "SeaTunnel job configuration file (required for submit unless the provider supplies it, for example kubernetes.job-config-file)")
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

    @Parameter(
            names = "--wait-timeout",
            description =
                    "Maximum time in milliseconds that --wait polls for a terminal status before failing with the last observed status; 0 waits indefinitely (default: 0)")
    private Long waitTimeoutMillis;

    @Parameter(
            names = "--wait-interval",
            description = "Status poll interval in milliseconds while --wait blocks (default: 500)")
    private Long waitIntervalMillis;

    @DynamicParameter(
            names = "-D",
            description = "Override a non-sensitive deployment option: -Dkey=value")
    private Map<String, String> options = new LinkedHashMap<>();

    @Override
    public Command<?> buildCommand() {
        validateCommandOptions();
        return new ApplicationExecuteCommand(this);
    }

    /**
     * Validates command-specific arguments before reading files or opening platform connections.
     *
     * @throws ParameterException when the combination of arguments is invalid, so the caller
     *     reports it through the usage path instead of a stack trace
     */
    public void validateCommandOptions() {
        if (applicationConfig != null) {
            requireFile(applicationConfig, "--application-config");
        }
        if (waitTimeoutMillis != null && waitTimeoutMillis < 0) {
            throw new ParameterException("--wait-timeout must not be negative");
        }
        if (waitIntervalMillis != null && waitIntervalMillis < 1) {
            throw new ParameterException("--wait-interval must be positive");
        }
        if (!wait && (waitTimeoutMillis != null || waitIntervalMillis != null)) {
            throw new ParameterException("--wait-timeout and --wait-interval require --wait");
        }
        if (operation == ApplicationOperation.SUBMIT) {
            if (config != null) {
                requireFile(config, "--config");
            }
            if (id != null) {
                throw new ParameterException("submit does not accept --id");
            }
            return;
        }
        if (id == null || id.trim().isEmpty()) {
            throw new ParameterException("status/cancel require --id");
        }
        if (config != null) {
            throw new ParameterException("status/cancel do not accept --config");
        }
        if (jobId != null || restoreJobId != null) {
            throw new ParameterException(
                    "--job-id and --restore-job-id are supported only with submit");
        }
        if (wait && operation == ApplicationOperation.CANCEL) {
            throw new ParameterException("--wait is supported only with submit or status");
        }
    }

    /** @return configured wait timeout in milliseconds, or {@code 0} to wait indefinitely */
    public long resolvedWaitTimeoutMillis() {
        return waitTimeoutMillis == null ? 0L : waitTimeoutMillis;
    }

    /** @return configured poll interval in milliseconds, or the default interval */
    public long resolvedWaitIntervalMillis() {
        return waitIntervalMillis == null ? DEFAULT_WAIT_INTERVAL_MILLIS : waitIntervalMillis;
    }

    private static void requireFile(String path, String option) {
        if (!Files.isRegularFile(Paths.get(path))) {
            throw new ParameterException(option + " file does not exist: " + path);
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
            if (value != null && DeployType.KUBERNETES.name().equalsIgnoreCase(value.trim())) {
                return DeployType.KUBERNETES;
            }
            if (value != null && DeployType.STANDALONE.name().equalsIgnoreCase(value.trim())) {
                throw new ParameterException(
                        "Use seatunnel.sh for standalone jobs; the application target is kubernetes");
            }
            throw new ParameterException(
                    "Unsupported target '"
                            + value
                            + "'. Currently only [kubernetes] is supported!");
        }
    }
}
