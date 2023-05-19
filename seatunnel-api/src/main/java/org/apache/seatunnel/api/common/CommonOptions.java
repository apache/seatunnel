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

package org.apache.seatunnel.api.common;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public interface CommonOptions {
    Option<String> FACTORY_ID =
            Options.key("factory")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Identifier of the SPI factory class.")
                    .withFallbackKeys("plugin_name");

    Option<String> PLUGIN_NAME =
            Options.key("plugin_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the SPI plugin class.");

    Option<String> RESULT_TABLE_NAME =
            Options.key("result_table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "When result_table_name is not specified, "
                                    + "the data processed by this plugin will not be registered as a data set (dataStream/dataset) "
                                    + "that can be directly accessed by other plugins, or called a temporary table (table)"
                                    + "When result_table_name is specified, "
                                    + "the data processed by this plugin will be registered as a data set (dataStream/dataset) "
                                    + "that can be directly accessed by other plugins, or called a temporary table (table) . "
                                    + "The data set (dataStream/dataset) registered here can be directly accessed by other plugins "
                                    + "by specifying source_table_name .");

    Option<List<String>> SOURCE_TABLE_NAME =
            Options.key("source_table_name")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "When source_table_name is not specified, "
                                    + "the current plug-in processes the data set dataset output by the previous plugin in the configuration file. "
                                    + "When source_table_name is specified, the current plug-in is processing the data set corresponding to this parameter.");

    Option<Integer> PARALLELISM =
            Options.key("parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "When parallelism is not specified, the parallelism in env is used by default. "
                                    + "When parallelism is specified, it will override the parallelism in env.");
}
