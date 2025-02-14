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

package org.apache.seatunnel.api.options;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.SaveModeExecuteLocation;
import org.apache.seatunnel.common.constants.JobMode;

import java.util.Map;

public class EnvCommonOptions {
    public static Option<Integer> PARALLELISM =
            Options.key("parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "When parallelism is not specified in connector, the parallelism in env is used by default. "
                                    + "When parallelism is specified, it will override the parallelism in env.");

    public static Option<String> JOB_NAME =
            Options.key("job.name")
                    .stringType()
                    .defaultValue("SeaTunnel_Job")
                    .withDescription("The job name of this job");

    public static Option<JobMode> JOB_MODE =
            Options.key("job.mode")
                    .enumType(JobMode.class)
                    .defaultValue(JobMode.BATCH)
                    .withDescription("The job mode of this job, support Batch and Stream");

    public static Option<Integer> JOB_RETRY_TIMES =
            Options.key("job.retry.times")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The retry times of this job");

    public static Option<Integer> JOB_RETRY_INTERVAL_SECONDS =
            Options.key("job.retry.interval.seconds")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The retry interval seconds of this job");

    public static Option<Long> CHECKPOINT_INTERVAL =
            Options.key("checkpoint.interval")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The interval (in milliseconds) between two consecutive checkpoints.");

    public static Option<Integer> READ_LIMIT_ROW_PER_SECOND =
            Options.key("read_limit.rows_per_second")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The each parallelism row limit per second for read data from source.");

    public static Option<Integer> READ_LIMIT_BYTES_PER_SECOND =
            Options.key("read_limit.bytes_per_second")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The each parallelism bytes limit per second for read data from source.");

    public static Option<Long> CHECKPOINT_TIMEOUT =
            Options.key("checkpoint.timeout")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The timeout (in milliseconds) for a checkpoint.");

    public static Option<SaveModeExecuteLocation> SAVEMODE_EXECUTE_LOCATION =
            Options.key("savemode.execute.location")
                    .enumType(SaveModeExecuteLocation.class)
                    .defaultValue(SaveModeExecuteLocation.CLUSTER)
                    .withDescription("The location of save mode execute.");

    public static Option<String> JARS =
            Options.key("jars")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("third-party packages can be loaded via `jars`");

    public static Option<Map<String, String>> CUSTOM_PARAMETERS =
            Options.key("custom_parameters")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("custom parameters for run engine");

    public static Option<Map<String, String>> NODE_TAG_FILTER =
            Options.key("tag_filter")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Define the worker where the job runs by tag");
}
