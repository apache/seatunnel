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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import java.io.Serializable;

public class MaxcomputeConfig implements Serializable {

    public static final String PLUGIN_NAME = "Maxcompute";

    private static final int SPLIT_ROW_DEFAULT = 10000;
    public static final Option<String> ACCESS_ID =
            Options.key("accessId")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Your Maxcompute accessId which cloud be access from Alibaba Cloud");
    public static final Option<String> ACCESS_KEY =
            Options.key("accesskey")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Your Maxcompute accessKey which cloud be access from Alibaba Cloud");
    public static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Your Maxcompute endpoint start with http");
    public static final Option<String> PROJECT =
            Options.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Your Maxcompute project which is created in Alibaba Cloud");
    public static final Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target Maxcompute table name eg: fake");
    public static final Option<String> PARTITION_SPEC =
            Options.key("partition_spec")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("This spec of Maxcompute partition table.");
    public static final Option<Integer> SPLIT_ROW =
            Options.key("split_row")
                    .intType()
                    .defaultValue(SPLIT_ROW_DEFAULT)
                    .withDescription("Number of rows per split. default: 10000");
    public static final Option<Boolean> OVERWRITE =
            Options.key("overwrite")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to overwrite the table or partition");

    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription("schema_save_mode");

    public static final Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(DataSaveMode.APPEND_DATA)
                    .withDescription("data_save_mode");

    public static final Option<String> CUSTOM_SQL =
            Options.key("custom_sql").stringType().noDefaultValue().withDescription("custom_sql");

    // create table
    public static final Option<String> SAVE_MODE_CREATE_TEMPLATE =
            Options.key("save_mode_create_template")
                    .stringType()
                    .defaultValue(
                            "CREATE TABLE IF NOT EXISTS `"
                                    + SaveModePlaceHolder.TABLE.getPlaceHolder()
                                    + "` (\n"
                                    + SaveModePlaceHolder.ROWTYPE_FIELDS.getPlaceHolder()
                                    + "\n"
                                    + ") COMMENT '"
                                    + SaveModePlaceHolder.COMMENT.getPlaceHolder()
                                    + "' ;")
                    .withDescription(
                            "Create table statement template, used to create MaxCompute table");
}
