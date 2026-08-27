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

package org.apache.seatunnel.connectors.seatunnel.tdengine.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class TDengineSourceOptions extends TDengineCommonOptions {

    public static final Option<String> LOWER_BOUND =
            Options.key("lower_bound")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The lower bound for data query range");

    public static final Option<String> UPPER_BOUND =
            Options.key("upper_bound")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The upper bound for data query range");

    public static final Option<List<String>> SUB_TABLES =
            Options.key("sub_tables")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "The sub table names to query data from, separated by comma , "
                                    + "if not specified, all sub tables will be queried");

    public static final Option<List<String>> READ_COLUMNS =
            Options.key("read_columns")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "The field names to be read from TDengine "
                                    + "If not specified, all columns will be read. "
                                    + "This option is useful for selecting specific columns when querying data from TDengine.");
}
