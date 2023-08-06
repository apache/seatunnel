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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

public interface JdbcCatalogOptions {
    Option<String> BASE_URL =
            Options.key("base-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "URL has to be with database, like \"jdbc:mysql://localhost:5432/db\" or"
                                    + "\"jdbc:mysql://localhost:5432/db?useSSL=true\".");

    Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the database to use when connecting to the database server.");

    Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to use when connecting to the database server.");

    Option<String> SCHEMA =
            Options.key("schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "for databases that support the schema parameter, give it priority.");

    OptionRule.Builder BASE_RULE =
            OptionRule.builder().required(BASE_URL).required(USERNAME, PASSWORD).optional(SCHEMA);

    Option<String> TABLE_PREFIX =
            Options.key("tablePrefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table prefix name added when the table is automatically created");

    Option<String> TABLE_SUFFIX =
            Options.key("tableSuffix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table suffix name added when the table is automatically created");
}
