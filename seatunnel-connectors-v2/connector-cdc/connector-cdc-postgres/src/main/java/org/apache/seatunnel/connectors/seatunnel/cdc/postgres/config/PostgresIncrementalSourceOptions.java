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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;

import java.util.List;

public class PostgresIncrementalSourceOptions extends JdbcSourceOptions {

    public static final Option<String> DECODING_PLUGIN_NAME =
            Options.key("decoding.plugin.name")
                    .stringType()
                    .defaultValue("pgoutput")
                    .withDescription(
                            "The name of the Postgres logical decoding plug-in installed on the server.\n"
                                    + "Supported values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming,\n"
                                    + "wal2json_rds_streaming and pgoutput.");

    public static final Option<String> SLOT_NAME =
            Options.key("slot.name")
                    .stringType()
                    .defaultValue("seatunnel")
                    .withDescription(
                            "The name of the PostgreSQL logical decoding slot that was created for streaming changes "
                                    + "from a particular plug-in for a particular database/schema. The server uses this slot "
                                    + "to stream events to the connector that you are configuring. Default is \"seatunnel\".");

    public static final Option<List<String>> SCHEMA_NAME =
            Options.key("schema-name")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Schema name of the database to monitor.");

    public static final Option<Boolean> REQUIRE_REPLICA_IDENTITY_FULL =
            Options.key("require-replica-identity-full")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Require the table to have REPLICA IDENTITY FULL. "
                                    + "When set to false, allows tables with other replica identity settings, "
                                    + "but UPDATE/DELETE events may not contain the previous state. "
                                    + "This should only be used for append-only tables (e.g., outbox pattern). "
                                    + "Default is true for backward compatibility.");

    public static final Option<String> SSL_MODE =
            Options.key("sslmode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "SSL mode for the Debezium PostgreSQL replication and data connections. "
                                    + "Valid values: disable, allow, prefer, require, verify-ca, verify-full. "
                                    + "When set, it is written to database.sslmode and overrides "
                                    + "debezium.database.sslmode. When unset, database.sslmode is left untouched "
                                    + "(the JDBC url sslmode is not inherited), so debezium.database.sslmode "
                                    + "applies if configured and otherwise the Debezium default disable is used.");

    public static final Option<String> PUBLICATION_NAME =
            Options.key("publication.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The PostgreSQL publication name used for pgoutput logical replication "
                                    + "(defaults to dbz_publication when unset).");

    public static final Option<String> PUBLICATION_AUTOCREATE_MODE =
            Options.key("publication.autocreate.mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Controls how the publication specified by publication.name is created when using pgoutput. "
                                    + "Valid values: all_tables (default, creates FOR ALL TABLES), "
                                    + "filtered (creates FOR TABLE for monitored tables only), "
                                    + "disabled (uses an existing publication without creating or altering it).");
}
