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

package org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;

import io.debezium.connector.vitess.connection.VitessTabletType;

import java.util.Arrays;

/** Source options owned by the Vitess CDC connector. */
public final class VitessSourceOptions {

    /** VTGate hostname used by the VStream gRPC client. */
    public static final Option<String> HOSTNAME =
            Options.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname or IP address of the Vitess VTGate gRPC server.");

    /** VTGate VStream gRPC port. */
    public static final Option<Integer> PORT =
            Options.key("port")
                    .intType()
                    .defaultValue(15991)
                    .withDescription("Port of the Vitess VTGate gRPC server.");

    /** Optional VTGate username when the cluster enables authentication. */
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username used by the Vitess VTGate gRPC connection.");

    /** Optional VTGate password when the cluster enables authentication. */
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password used by the Vitess VTGate gRPC connection.");

    /** Single keyspace captured by one connector instance. */
    public static final Option<String> KEYSPACE =
            Options.key("keyspace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Vitess keyspace to capture.");

    /**
     * Optional shard restriction for deployments that intentionally bind one connector to one
     * shard.
     */
    public static final Option<String> SHARD =
            Options.key("shard")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional shard restriction. When omitted, the connector captures all shards in the configured keyspace.");

    /**
     * Startup mode is intentionally narrow so the first delivery keeps startup semantics explicit.
     */
    public static final Option<StartupMode> STARTUP_MODE =
            Options.key(SourceOptions.STARTUP_MODE_KEY)
                    .singleChoice(
                            StartupMode.class,
                            Arrays.asList(StartupMode.LATEST, StartupMode.SPECIFIC))
                    .defaultValue(StartupMode.LATEST)
                    .withDescription(
                            "Startup mode for Vitess CDC. Supported values are "
                                    + "\"latest\" and \"specific\". "
                                    + "\"specific\" is the stable startup path for reproducible restore semantics.");

    /** Stable startup VGTID used when startup.mode=specific. */
    public static final Option<String> STARTUP_SPECIFIC_OFFSET_VGTID =
            Options.key("startup.specific-offset.vgtid")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Vitess VGTID used when startup.mode is set to specific.");

    /** The tablet type used by Vitess streaming. */
    public static final Option<VitessTabletType> TABLET_TYPE =
            Options.key("tablet-type")
                    .enumType(VitessTabletType.class)
                    .defaultValue(VitessTabletType.MASTER)
                    .withDescription(
                            "Vitess tablet type used by VStream. Supported values are MASTER, REPLICA and RDONLY.");

    /** Whether VStream should stop after resharding. */
    public static final Option<Boolean> STOP_ON_RESHARD =
            Options.key("stop-on-reshard")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether the connector should stop after Vitess resharding.");

    /** Optional gRPC keepalive interval. */
    public static final Option<Long> KEEPALIVE_INTERVAL_MS =
            Options.key("keepalive.interval.ms")
                    .longType()
                    .defaultValue(Long.MAX_VALUE)
                    .withDescription(
                            "VStream gRPC keepalive interval in milliseconds. Long.MAX_VALUE disables keepalive.");

    /** Optional raw gRPC headers passed through to VTGate. */
    public static final Option<String> GRPC_HEADERS =
            Options.key("grpc.headers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional comma-separated gRPC headers in key:value format.");

    /** Maximum inbound gRPC message size. */
    public static final Option<Integer> GRPC_MAX_INBOUND_MESSAGE_SIZE =
            Options.key("grpc.max-inbound-message-size")
                    .intType()
                    .defaultValue(4_194_304)
                    .withDescription("Maximum inbound VStream gRPC message size in bytes.");

    /** Time zone used by SeaTunnel row deserialization for temporal normalization. */
    public static final Option<String> SERVER_TIME_ZONE =
            Options.key("server-time-zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription("Time zone used by SeaTunnel row deserialization.");

    private VitessSourceOptions() {}
}
