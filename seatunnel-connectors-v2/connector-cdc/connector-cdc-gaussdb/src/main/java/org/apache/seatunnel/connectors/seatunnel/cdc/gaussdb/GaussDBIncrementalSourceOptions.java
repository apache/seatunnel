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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;

/** User-facing options for GaussDB logical decoding. */
public final class GaussDBIncrementalSourceOptions extends JdbcSourceOptions {

    /** Logical decoding plugin installed in GaussDB. */
    public static final Option<String> DECODING_PLUGIN_NAME =
            Options.key("decoding.plugin.name")
                    .stringType()
                    .defaultValue("mppdb_decoding")
                    .withDescription(
                            "Logical decoding plugin. GaussDB-CDC provides a native reader for mppdb_decoding and also supports PostgreSQL-compatible Debezium plugins such as pgoutput.");

    /** Logical replication slot used by the CDC job. */
    public static final Option<String> SLOT_NAME =
            Options.key("slot.name")
                    .stringType()
                    .defaultValue("seatunnel")
                    .withDescription(
                            "Logical replication slot name. Each concurrent CDC job must use a distinct slot.");

    /** Optional dedicated GaussDB replication port. */
    public static final Option<Integer> REPLICATION_PORT =
            Options.key("replication.port")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Dedicated port used by the replication protocol. When omitted, the port in url is used.");

    /** Number of server-side mppdb decoder workers. */
    public static final Option<Integer> PARALLEL_DECODE_NUM =
            Options.key("parallel-decode-num")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Number of mppdb_decoding decoder workers. The supported range is 1 through 20.");

    /** Server-side mppdb output representation for parallel decoding. */
    public static final Option<String> DECODE_STYLE =
            Options.key("decode-style")
                    .stringType()
                    .defaultValue("b")
                    .withDescription(
                            "mppdb_decoding output style when parallel-decode-num is greater than 1: b for binary, j for JSON, or t for text.");

    /** Whether mppdb_decoding groups output into server-side batches. */
    public static final Option<Boolean> SENDING_BATCH =
            Options.key("sending-batch")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether mppdb_decoding sends accumulated batches. This option is effective when parallel-decode-num is greater than 1.");

    private GaussDBIncrementalSourceOptions() {}
}
