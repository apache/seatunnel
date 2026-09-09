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

package org.apache.seatunnel.connectors.seatunnel.couchbase.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

/** Shared Couchbase connection options used by all connector roles. */
public class CouchbaseConfig extends ConnectorCommonOptions {

    public static final String CONNECTOR_IDENTITY = "Couchbase";

    /** Couchbase connection string, e.g. {@code couchbase://localhost}. */
    public static final Option<String> CONNECTION_STRING =
            Options.key("connection.string")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Couchbase connection string, e.g. couchbase://localhost.");

    /** Couchbase username for authentication. */
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Couchbase username used for authentication.");

    /** Couchbase password for authentication. */
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Couchbase password used for authentication.");

    /** Target bucket name. */
    public static final Option<String> BUCKET =
            Options.key("bucket")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the Couchbase bucket to write to.");

    /** Target scope name. Defaults to the default scope. */
    public static final Option<String> SCOPE =
            Options.key("scope")
                    .stringType()
                    .defaultValue("_default")
                    .withDescription(
                            "The name of the Couchbase scope within the bucket. Defaults to _default.");

    /** Target collection name. */
    public static final Option<String> COLLECTION =
            Options.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the Couchbase collection to write to.");
}
