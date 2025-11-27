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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class HugeGraphSinkOptions {

    public static final Option<List<String>> SELECTED_FIELDS =
            Options.key("selected_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Selected Fields");

    public static final Option<List<String>> IGNORED_FIELDS =
            Options.key("ignored_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Ignored Fields");

    public static final Option<SchemaConfig> SCHEMA_CONFIG =
            Options.key("schema_config")
                    .objectType(SchemaConfig.class)
                    .noDefaultValue()
                    .withDescription(
                            "Schema configuration object that describes the mapping to a vertex or edge.");
}
