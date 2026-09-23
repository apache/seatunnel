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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationResult;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.client.ApplicationClient;
import org.apache.seatunnel.resource.core.client.ApplicationDeployer;
import org.apache.seatunnel.resource.core.client.ApplicationDeployers;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** CLI for externally managed, single-job Zeta applications. */
public final class SeaTunnelApplication {
    private SeaTunnelApplication() {}

    public static void main(String[] args) {
        int result;
        try {
            result = execute(args, System.out);
        } catch (Exception e) {
            System.err.println("Application command failed: " + e.getMessage());
            result = 1;
        }
        if (result != 0) {
            System.exit(result);
        }
    }

    /** Executes one deployment operation; detached submission leaves the application running. */
    public static int execute(String[] args, PrintStream output) throws Exception {
        Arguments arguments = new Arguments();
        JCommander parser =
                JCommander.newBuilder()
                        .addObject(arguments)
                        .programName("seatunnel-application.sh")
                        .build();
        parser.parse(args);
        if (arguments.help || args.length == 0) {
            StringBuilder usage = new StringBuilder();
            parser.getUsageFormatter().usage(usage);
            output.print(usage);
            return 0;
        }
        arguments.validate();
        DeployType type = DeployType.valueOf(arguments.target.toUpperCase(Locale.ROOT));
        if (type == DeployType.STANDALONE) {
            throw new IllegalArgumentException(
                    "Use seatunnel.sh for standalone jobs; application targets are yarn and kubernetes");
        }
        Map<String, String> options = loadOptions(arguments.deploymentConfig);
        options.putAll(arguments.options);
        if (arguments.jobId != null) {
            options.put(ApplicationOptions.JOB_ID.key(), arguments.jobId.toString());
        }
        if (arguments.restoreJobId != null) {
            options.put(ApplicationOptions.RESTORE_JOB_ID.key(), arguments.restoreJobId.toString());
        }
        String operation = arguments.operation.get(0);
        ApplicationSpecification specification = null;
        if ("submit".equals(operation)) {
            requireFile(arguments.config);
            String jobConfig =
                    ConfigFactory.parseFile(Paths.get(arguments.config).toFile())
                            .resolve()
                            .root()
                            .render(ConfigRenderOptions.concise());
            specification = ApplicationSpecification.fromOptions(type, jobConfig, options);
        }
        try (ApplicationDeployer deployer = ApplicationDeployers.create(type, options)) {
            try (ApplicationClient client =
                    "submit".equals(operation)
                            ? deployer.deploy(specification)
                            : deployer.retrieve(new ApplicationId(type, arguments.id), options)) {
                output.println("Application ID: " + client.getApplicationId().getId());
                if (specification != null) {
                    output.println("Job ID: " + specification.getJobId());
                }
                if ("cancel".equals(operation)) {
                    client.cancel();
                    output.println("Cancellation requested");
                    return 0;
                }
                return printResult(client, arguments.wait, output);
            }
        }
    }

    static int printResult(ApplicationClient client, boolean wait, PrintStream output)
            throws Exception {
        ApplicationResult result = client.getResult();
        while (wait
                && !result.getStatus().isTerminal()
                && result.getStatus() != ApplicationStatus.UNKNOWN) {
            Thread.sleep(1000L);
            result = client.getResult();
        }
        output.println("Status: " + result.getStatus());
        if (!result.getDiagnostics().isEmpty()) {
            output.println(result.getDiagnostics());
        }
        return result.getStatus() == ApplicationStatus.FAILED
                        || result.getStatus() == ApplicationStatus.CANCELED
                        || result.getStatus() == ApplicationStatus.UNKNOWN
                ? 1
                : 0;
    }

    static Map<String, String> loadOptions(String path) {
        Map<String, String> options = new LinkedHashMap<>();
        if (path != null) {
            requireFile(path);
            Config config = ConfigFactory.parseFile(Paths.get(path).toFile()).resolve();
            for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
                Object value = entry.getValue().unwrapped();
                if (!(value instanceof String
                        || value instanceof Number
                        || value instanceof Boolean)) {
                    throw new IllegalArgumentException(
                            "Deployment option must be scalar: " + entry.getKey());
                }
                // Config.entrySet() returns HOCON paths, which quote keys containing hyphens.
                String key = String.join(".", ConfigUtil.splitPath(entry.getKey()));
                options.put(key, String.valueOf(value));
            }
        }
        return options;
    }

    private static void requireFile(String path) {
        if (path == null || !Files.isRegularFile(Paths.get(path))) {
            throw new IllegalArgumentException("Configuration file does not exist: " + path);
        }
    }

    private static final class Arguments {
        @Parameter(description = "submit | status | cancel")
        private List<String> operation = new ArrayList<>();

        @Parameter(
                names = "--target",
                description = "External resource platform: yarn or kubernetes")
        private String target;

        @Parameter(names = "--config", description = "SeaTunnel job configuration file")
        private String config;

        @Parameter(
                names = "--job-id",
                description = "Optional positive native Zeta job ID for this submission")
        private Long jobId;

        @Parameter(
                names = "--restore-from-checkpoint",
                description =
                        "Historical Zeta job ID whose latest checkpoint restores this new application")
        private Long restoreJobId;

        @Parameter(
                names = "--deployment-config",
                description = "Platform deployment options in HOCON format")
        private String deploymentConfig;

        @Parameter(names = "--id", description = "External application ID returned by submit")
        private String id;

        @Parameter(names = "--wait", description = "Wait for terminal application status")
        private boolean wait;

        @DynamicParameter(names = "-D", description = "Override a deployment option: -Dkey=value")
        private Map<String, String> options = new LinkedHashMap<>();

        @Parameter(
                names = {"--help", "-h"},
                help = true)
        private boolean help;

        private void validate() {
            if (operation.size() != 1
                    || !("submit".equals(operation.get(0))
                            || "status".equals(operation.get(0))
                            || "cancel".equals(operation.get(0)))) {
                throw new IllegalArgumentException(
                        "Specify exactly one command: submit, status or cancel");
            }
            if (target == null) {
                throw new IllegalArgumentException("--target is required");
            }
            if (!"submit".equals(operation.get(0)) && (jobId != null || restoreJobId != null)) {
                throw new IllegalArgumentException(
                        "--job-id and --restore-from-checkpoint are supported only with submit");
            }
            if ("submit".equals(operation.get(0))) {
                if (config == null || id != null) {
                    throw new IllegalArgumentException(
                            "submit requires --config and does not accept --id");
                }
            } else if (id == null || config != null) {
                throw new IllegalArgumentException(
                        "status/cancel require --id and do not accept --config");
            }
            if (wait && !"submit".equals(operation.get(0)) && !"status".equals(operation.get(0))) {
                throw new IllegalArgumentException(
                        "--wait is supported only with submit or status");
            }
        }
    }
}
