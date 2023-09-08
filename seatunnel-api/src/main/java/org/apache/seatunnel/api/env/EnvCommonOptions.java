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

package org.apache.seatunnel.api.env;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.common.constants.JobMode;

import java.util.Map;

public interface EnvCommonOptions {
    Option<Integer> PARALLELISM =
            Options.key("parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "When parallelism is not specified in connector, the parallelism in env is used by default. "
                                    + "When parallelism is specified, it will override the parallelism in env.");

    Option<String> JOB_NAME =
            Options.key("job.name")
                    .stringType()
                    .defaultValue("SeaTunnel_Job")
                    .withDescription("The job name of this job");

    Option<JobMode> JOB_MODE =
            Options.key("job.mode")
                    .enumType(JobMode.class)
                    .defaultValue(JobMode.BATCH)
                    .withDescription("The job mode of this job, support Batch and Stream");

    Option<Long> CHECKPOINT_INTERVAL =
            Options.key("checkpoint.interval")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The interval (in milliseconds) between two consecutive checkpoints.");

    Option<Long> CHECKPOINT_TIMEOUT =
            Options.key("checkpoint.timeout")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The timeout (in milliseconds) for a checkpoint.");

    Option<String> JARS =
            Options.key("jars")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("third-party packages can be loaded via `jars`");

    Option<Map<String, String>> CUSTOM_PARAMETERS =
            Options.key("custom_parameters")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("custom parameters for run engine");
}
