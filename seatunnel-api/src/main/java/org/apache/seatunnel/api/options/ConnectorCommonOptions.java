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

package org.apache.seatunnel.api.options;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.table.CatalogOptions;
import org.apache.seatunnel.api.options.table.ColumnOptions;
import org.apache.seatunnel.api.options.table.ConstraintKeyOptions;
import org.apache.seatunnel.api.options.table.FieldOptions;
import org.apache.seatunnel.api.options.table.PrimaryKeyOptions;
import org.apache.seatunnel.api.options.table.TableIdentifierOptions;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;

import java.io.Serializable;
import java.util.List;

public class ConnectorCommonOptions
        implements CatalogOptions,
                TableSchemaOptions,
                TableIdentifierOptions,
                FieldOptions,
                ColumnOptions,
                PrimaryKeyOptions,
                ConstraintKeyOptions,
                Serializable {

    public static Option<String> PLUGIN_NAME =
            Options.key("plugin_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the SPI plugin class.");

    public static Option<String> PLUGIN_OUTPUT =
            Options.key("plugin_output")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("result_table_name")
                    .withDescription(
                            "When plugin_output is not specified, "
                                    + "the data processed by this plugin will not be registered as a data set (dataStream/dataset) "
                                    + "that can be directly accessed by other plugins, or called a temporary table (table)"
                                    + "When plugin_output is specified, "
                                    + "the data processed by this plugin will be registered as a data set (dataStream/dataset) "
                                    + "that can be directly accessed by other plugins, or called a temporary table (table) . "
                                    + "The data set (dataStream/dataset) registered here can be directly accessed by other plugins "
                                    + "by specifying plugin_input .");

    public static Option<List<String>> PLUGIN_INPUT =
            Options.key("plugin_input")
                    .listType()
                    .noDefaultValue()
                    .withFallbackKeys("source_table_name")
                    .withDescription(
                            "When plugin_input is not specified, "
                                    + "the current plug-in processes the data set dataset output by the previous plugin in the configuration file. "
                                    + "When plugin_input is specified, the current plug-in is processing the data set corresponding to this parameter.");
}
