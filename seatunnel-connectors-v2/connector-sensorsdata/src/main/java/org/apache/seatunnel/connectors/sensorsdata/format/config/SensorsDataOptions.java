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

package org.apache.seatunnel.connectors.sensorsdata.format.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

@SuppressWarnings("checkstyle:MagicNumber")
public interface SensorsDataOptions {
    Option<String> ENTITY_NAME =
            Options.key("entity_name")
                    .stringType()
                    .defaultValue("users")
                    .withDescription("entity name: users(default)/items");

    Option<String> RECORD_TYPE =
            Options.key("record_type")
                    .stringType()
                    .defaultValue("users")
                    .withDescription("Record type: users/events/items/details");

    Option<String> SCHEMA =
            Options.key("schema").stringType().defaultValue("users").withDescription("Schema name");

    Option<String> DISTINCT_ID_COLUMN =
            Options.key("distinct_id_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specify a column as the distinct id for users");

    Option<List<TargetColumnConfig>> IDENTITY_FIELDS =
            Options.key("identity_fields")
                    .listType(TargetColumnConfig.class)
                    .noDefaultValue()
                    .withDescription(
                            "Specify the identity fields and where they come from. format: { source = ${source_field}, target = ${identity_field} }");

    Option<List<TargetColumnConfig>> PROPERTY_FIELDS =
            Options.key("property_fields")
                    .listType(TargetColumnConfig.class)
                    .noDefaultValue()
                    .withDescription(
                            "Specify the property fields and their data types. format: { source = ${source_field}, target = ${target_property_field}, type = ${data_type} }");

    Option<String> EVENT_NAME =
            Options.key("event_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specify the event name when record_type = \"events\".");

    Option<String> TIME_COLUMN =
            Options.key("time_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specify a column as the $time property for events");

    Option<String> DETAIL_ID_COLUMN =
            Options.key("detail_id_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify a column as the detail id for detail entity when record_type = \"details\".");

    Option<Boolean> TIME_FREE =
            Options.key("time_free")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable time_free for events, true/false(default)");

    Option<List<TargetColumnConfig>> TARGET_COLUMNS =
            Options.key("target_columns").listType(TargetColumnConfig.class).noDefaultValue();

    Option<Boolean> SKIP_ERROR_RECORD =
            Options.key("skip_error_record")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "While encountering an error, either skip the record or terminate the process.");

    Option<String> ITEM_ID_COLUMN =
            Options.key("item_id_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify a column as the item id for items when record_type = \"items\".");

    Option<String> ITEM_TYPE_COLUMN =
            Options.key("item_type_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify a column as the item type for items when record_type = \"items\".");

    Option<String> JSON_COLUMN_NAME =
            Options.key("json_column_name")
                    .stringType()
                    .defaultValue("json_content")
                    .withDescription(
                            "Specify the target column name for the output of the SensorsDataJson Transform. ");

    Option<Boolean> DISTINCT_ID_BY_IDENTITIES =
            Options.key("distinct_id_by_identities")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "when distinct_id_column value is null, enable get distinctId value by identityFields");

    Option<Boolean> NULL_AS_PROFILE_UNSET =
            Options.key("null_as_profile_unset")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "when properties value is null, enable send profile_unset action");
}
