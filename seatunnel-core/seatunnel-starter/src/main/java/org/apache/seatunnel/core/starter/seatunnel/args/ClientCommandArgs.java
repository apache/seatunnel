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
import org.apache.seatunnel.core.starter.enums.DryRun;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.seatunnel.command.ClientExecuteCommand;
import org.apache.seatunnel.core.starter.seatunnel.command.SeaTunnelConfValidateCommand;
import org.apache.seatunnel.engine.common.config.DryRunSampleConfig;

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
            names = {"-d", "--dry-run"},
            description =
                    "Validate or preview without running sinks. Supported modes: [static, connect, sample].",
            converter = DryRunConverter.class)
    protected DryRun dryRun = null;

    @Parameter(
            names = {"--sample-limit"},
            description =
                    "Maximum rows forwarded from each source by sample dry-run mode (default: 10, max: 10000)",
            validateWith = PositiveIntegerValidator.class)
    private Integer sampleLimit;

    @Parameter(
            names = {"--sample-print-data"},
            description = "Print sampled row values to persistent logs")
    private boolean samplePrintData;

    @Parameter(
            names = {"-m", "--master", "-e", "--deploy-mode"},
            description = "SeaTunnel job submit master, support [local, cluster]",
            validateWith = MasterTypeValidator.class,
            converter = SeaTunnelMasterTargetConverter.class)
    private MasterType masterType = MasterType.CLUSTER;

    @Parameter(
            names = {"-r", "--restore", "--restore-job"},
            description = "restore with savepoint by jobId")
    private String restoreJobId;

    @Parameter(
            names = {"--restore-with-checkpoint"},
            description = "restore from latest successful completed checkpoint by historical jobId")
    private String restoreWithCheckpointJobId;

    @Parameter(
            names = {"-s", "--savepoint", "--savepoint-job"},
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
            names = {"-can", "--cancel", "--cancel-job"},
            variableArity = true,
            description = "Cancel job(s) by JobId")
    private List<String> cancelJobId;

    @Parameter(
            names = {"-f", "--force-cancel", "--force-cancel-job"},
            variableArity = true,
            description = "Force Cancel job(s) by JobId")
    private List<String> forceCancelJobId;

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
            names = {"--checkpoint-overview"},
            description = "Get checkpoint overview by JobId")
    private String checkpointOverviewJobId;

    @Parameter(
            names = {"--checkpoint-history"},
            description = "Get checkpoint history by JobId")
    private String checkpointHistoryJobId;

    @Parameter(
            names = {"--checkpoint-history-pipeline"},
            description = "Filter checkpoint history by pipeline id")
    private Integer checkpointHistoryPipeline;

    @Parameter(
            names = {"--checkpoint-history-limit"},
            description = "Limit checkpoint history size")
    private Integer checkpointHistoryLimit = 20;

    @Parameter(
            names = {"--checkpoint-history-status"},
            description = "Filter checkpoint history by status: COMPLETED,FAILED,CANCELED")
    private String checkpointHistoryStatus;

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
            names = {"-cj", "--close", "--close-job"},
            description = "Close client the task will also be closed")
    private boolean closeJob = true;

    @Override
    public Command<?> buildCommand() {
        validateCommandOptions();
        if (restoreJobId != null && restoreWithCheckpointJobId != null) {
            throw new IllegalArgumentException(
                    "--restore and --restore-with-checkpoint are mutually exclusive");
        }
        if (savePointJobId != null && restoreWithCheckpointJobId != null) {
            throw new IllegalArgumentException(
                    "--savepoint and --restore-with-checkpoint are mutually exclusive");
        }
        if (restoreWithCheckpointJobId != null) {
            restoreWithCheckpointJobId =
                    normalizeNumericJobId(
                            restoreWithCheckpointJobId,
                            "restoreSourceJobId is required when using --restore-with-checkpoint",
                            "--restore-with-checkpoint requires a numeric jobId, got: ");
        }
        if (customJobId != null) {
            customJobId =
                    normalizeNumericJobId(
                            customJobId,
                            "--set-job-id requires a non-blank jobId",
                            "--set-job-id requires a numeric jobId, got: ");
        }
        Common.setDeployMode(getDeployMode());
        if (dryRun == DryRun.SAMPLE) {
            return new ClientExecuteCommand(this);
        }
        if (checkConfig || dryRun != null) {
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

    private String normalizeNumericJobId(
            String value, String blankMessage, String invalidMessagePrefix) {
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(blankMessage);
        }
        try {
            Long.parseLong(trimmed);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(invalidMessagePrefix + value, e);
        }
        return trimmed;
    }

    public DeployMode getDeployMode() {
        return DeployMode.CLIENT;
    }

    public static class DryRunConverter implements IStringConverter<DryRun> {
        @Override
        public DryRun convert(String value) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException("Dry-run mode must not be empty.");
            }
            String trimmed = value.trim();
            if (DryRun.STATIC.getName().equalsIgnoreCase(trimmed)
                    || DryRun.STATIC.name().equalsIgnoreCase(trimmed)) {
                return DryRun.STATIC;
            }
            if (DryRun.CONNECT.getName().equalsIgnoreCase(trimmed)
                    || DryRun.CONNECT.name().equalsIgnoreCase(trimmed)) {
                return DryRun.CONNECT;
            }
            if (DryRun.SAMPLE.getName().equalsIgnoreCase(trimmed)
                    || DryRun.SAMPLE.name().equalsIgnoreCase(trimmed)) {
                return DryRun.SAMPLE;
            }
            throw new IllegalArgumentException(
                    "Unsupported dry-run mode '"
                            + value
                            + "'. Currently only [static, connect, sample] are supported; shadow"
                            + " is not implemented yet.");
        }
    }

    /** Returns the configured sample limit, or the default limit when it was not specified. */
    public int getSampleLimit() {
        return sampleLimit == null ? DryRunSampleConfig.DEFAULT_LIMIT : sampleLimit;
    }

    /** Validates options that depend on other command-line arguments. */
    public void validateCommandOptions() {
        validateSampleOptions();
        if (dryRun == DryRun.SAMPLE) {
            validateSampleMode();
        }
    }

    /**
     * Validates that sample mode runs locally without asynchronous submission, restore, savepoint,
     * validation, job control, encryption, or decryption options.
     *
     * @throws ParameterException when sample mode is combined with an unsupported option
     */
    public void validateSampleMode() {
        if (masterType != MasterType.LOCAL) {
            throw new ParameterException(
                    "Sample dry-run mode requires --master/--deploy-mode local.");
        }
        if (async) {
            throw new ParameterException("Sample dry-run mode does not support --async.");
        }
        if (restoreJobId != null
                || restoreWithCheckpointJobId != null
                || savePointJobId != null
                || checkConfig
                || listJob
                || getRunningJobMetrics
                || jobId != null
                || cancelJobId != null
                || forceCancelJobId != null
                || metricsJobId != null
                || checkpointOverviewJobId != null
                || checkpointHistoryJobId != null
                || encrypt
                || decrypt) {
            throw new ParameterException(
                    "Sample dry-run mode cannot be combined with validation, job control, restore, savepoint, encryption, or decryption options.");
        }
    }

    private void validateSampleOptions() {
        if (dryRun != DryRun.SAMPLE && (sampleLimit != null || samplePrintData)) {
            throw new ParameterException(
                    "--sample-limit and --sample-print-data require --dry-run sample.");
        }
    }

    /** Validates that a sample limit is between 1 and {@link DryRunSampleConfig#MAX_LIMIT}. */
    public static class PositiveIntegerValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            try {
                int limit = Integer.parseInt(value);
                if (limit < 1) {
                    throw new ParameterException(name + " must be greater than zero.");
                }
                if (limit > DryRunSampleConfig.MAX_LIMIT) {
                    throw new ParameterException(
                            name + " must not exceed " + DryRunSampleConfig.MAX_LIMIT + ".");
                }
            } catch (NumberFormatException e) {
                throw new ParameterException(name + " must be an integer.", e);
            }
        }
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
