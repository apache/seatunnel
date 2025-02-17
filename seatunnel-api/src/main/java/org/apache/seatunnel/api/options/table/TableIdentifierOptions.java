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

package org.apache.seatunnel.api.options.table;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public interface TableIdentifierOptions {

    Option<Boolean> SCHEMA_FIRST =
            Options.key("schema_first")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Parse Schema First from table");

    Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Full Table Name");

    Option<String> TABLE_COMMENT =
            Options.key("comment")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Table Comment");

    Option<String> DATABASE_NAME =
            Options.key("database_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Database Name");

    Option<String> SCHEMA_NAME =
            Options.key("schema_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Table Name");

    Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Table Name");
}
