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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.cdc.base.option.CdcJdbcBaseOptions;

import java.util.List;

public class OracleIncrementalSourceOptions extends CdcJdbcBaseOptions {

    public static final Option<List<String>> SCHEMA_NAMES =
            Options.key("schema-names")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Schema name of the database to monitor.");

    public static final Option<Boolean> USE_SELECT_COUNT =
            Options.key("use_select_count")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Use select count for table count in full stage");

    public static final Option<Boolean> SKIP_ANALYZE =
            Options.key("skip_analyze")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Skip the analysis of table count in full stage");
}
