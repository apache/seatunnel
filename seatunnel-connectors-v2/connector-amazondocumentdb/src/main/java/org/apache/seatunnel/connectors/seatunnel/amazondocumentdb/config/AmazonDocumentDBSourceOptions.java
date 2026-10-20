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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import java.io.Serializable;

/** Option definitions shared by factory validation and the DocumentDB runtime configuration. */
public class AmazonDocumentDBSourceOptions extends ConnectorCommonOptions implements Serializable {

    public static final Option<String> URI =
            Options.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Amazon DocumentDB connection URI, including endpoint and credentials");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Amazon DocumentDB database name");

    public static final Option<String> COLLECTION =
            Options.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Amazon DocumentDB collection name");

    public static final Option<Boolean> TLS =
            Options.key("tls")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to enable TLS for the Amazon DocumentDB connection");

    public static final Option<String> TLS_CA_FILE =
            Options.key("tls_ca_file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Path to a PEM CA bundle used to verify Amazon DocumentDB");

    public static final Option<String> MATCH_QUERY =
            Options.key("match.query")
                    .stringType()
                    .defaultValue("{}")
                    .withDescription("BSON query document used to filter source records");

    public static final Option<String> PROJECTION =
            Options.key("match.projection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("BSON projection document used to select source fields");

    public static final Option<Integer> FETCH_SIZE =
            Options.key("fetch.size")
                    .intType()
                    .defaultValue(2048)
                    .withDescription("Number of documents requested from the server per batch");
}
