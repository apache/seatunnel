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

package org.apache.seatunnel.api.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class SourceCommonOptions {

    public static final Option<String> RESULT_TABLE_NAME =
        Options.key("result_table_name")
            .stringType()
            .noDefaultValue()
            .withDescription(
                "When result_table_name is not specified, " +
                    "the data processed by this plugin will not be registered as a data set (dataStream/dataset) " +
                    "that can be directly accessed by other plugins, or called a temporary table (table)" +
                    "When result_table_name is specified, " +
                    "the data processed by this plugin will be registered as a data set (dataStream/dataset) " +
                    "that can be directly accessed by other plugins, or called a temporary table (table) . " +
                    "The data set (dataStream/dataset) registered here can be directly accessed by other plugins " +
                    "by specifying source_table_name .");

    public static final Option<Integer> PARALLELISM =
        Options.key("parallelism")
            .intType()
            .noDefaultValue()
            .withDescription("When parallelism is not specified, the parallelism in env is used by default. " +
                "When parallelism is specified, it will override the parallelism in env.");
}
