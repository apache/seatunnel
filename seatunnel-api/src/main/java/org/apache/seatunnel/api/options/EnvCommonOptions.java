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
import org.apache.seatunnel.common.constants.MetaLakeType;

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

    public static Option<Long> SINK_FLUSH_INTERVAL =
            Options.key("sink.flush.interval")
                    .longType()
                    .defaultValue(0L)
                    .withDescription(
                            "Interval (ms) at which the engine injects a FlushSignal into the pipeline to "
                                    + "drive a flush at the Sink. 0 means disabled. Values below 100ms will log a WARN.");

    public static Option<Integer> CHECKPOINT_MIN_PAUSE =
            Options.key("min-pause")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The minimum pause (in milliseconds) between consecutive checkpoints. "
                                    + "This ensures that checkpoints are not triggered too frequently and provides.");

    public static Option<Boolean> CHECKPOINT_RETAIN_AFTER_JOB_CANCELLED =
            Options.key("checkpoint.retain-after-job-cancelled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether to retain completed checkpoint data after this job is cancelled. "
                                    + "If configured, this job-level option overrides the cluster default.");

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

    public static Option<Boolean> METALAKE_ENABLED =
            Options.key("metalake_enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Turn on metadata lake");

    public static Option<MetaLakeType> METALAKE_TYPE =
            Options.key("metalake_type")
                    .enumType(MetaLakeType.class)
                    .defaultValue(MetaLakeType.GRAVITINO)
                    .withDescription("Metadata lake type, for example: gravitino");

    public static Option<String> METALAKE_URL =
            Options.key("metalake_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The http path of the metadata lake, for example: http://localhost:8090/api/metalakes/laowang_test/catalogs/");

    public static Option<Boolean> OPENLINEAGE_ENABLED =
            Options.key("openlineage_enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether OpenLineage lineage reporting is enabled.");

    public static Option<String> OPENLINEAGE_TRANSPORT =
            Options.key("openlineage_transport")
                    .stringType()
                    .defaultValue("http")
                    .withDescription(
                            "Transport name selected from the LineageBackend service provider interface.");

    public static Option<String> OPENLINEAGE_URL =
            Options.key("openlineage_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Base URL of the OpenLineage receiver, for example: http://host:8090/api/lineage");

    public static Option<String> OPENLINEAGE_NAMESPACE =
            Options.key("openlineage_namespace")
                    .stringType()
                    .defaultValue("seatunnel")
                    .withDescription("OpenLineage job namespace.");

    public static Option<String> OPENLINEAGE_AUTH_TOKEN =
            Options.key("openlineage_auth_token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Bearer token for the OpenLineage receiver. It may only come from the "
                                    + "OPENLINEAGE_AUTH_TOKEN environment variable or from cluster configuration; "
                                    + "declaring it in a job env block is rejected at startup.");

    public static Option<Integer> OPENLINEAGE_TIMEOUT_MS =
            Options.key("openlineage_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Timeout of one send attempt, in milliseconds.");

    public static Option<Integer> OPENLINEAGE_RETRY_TIMES =
            Options.key("openlineage_retry_times")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Number of retries after a failed send attempt.");

    public static Option<String> OPENLINEAGE_RUN_FACET =
            Options.key("openlineage_run_facet")
                    .stringType()
                    .defaultValue("seatunnel_properties")
                    .withDescription("Name of the run facet carrying the custom run properties.");

    public static Option<Map<String, String>> OPENLINEAGE_RUN_PROPERTIES =
            Options.key("openlineage_run_properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Custom properties copied into the run facet.");

    public static Option<Long> OPENLINEAGE_HEARTBEAT_MIN_INTERVAL_MS =
            Options.key("openlineage_heartbeat_min_interval_ms")
                    .longType()
                    .defaultValue(3600000L)
                    .withDescription(
                            "Minimum interval between streaming-job heartbeat events, in milliseconds.");

    public static Option<String> OPENLINEAGE_PRODUCER =
            Options.key("openlineage_producer")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "OpenLineage producer identifier. When unset it defaults to "
                                    + "https://seatunnel.apache.org/ followed by the running SeaTunnel version.");
}
