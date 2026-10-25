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

package org.apache.seatunnel.connectors.seatunnel.airtable.source.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.airtable.config.AirtableConfig;

import java.util.List;

public class AirtableSourceOptions extends AirtableConfig {

    public static final Option<String> VIEW =
            Options.key("view")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name or ID of a view");

    public static final Option<List<String>> FIELDS =
            Options.key("fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("The list of field names to include");

    public static final Option<String> FILTER_BY_FORMULA =
            Options.key("filter_by_formula")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Airtable filterByFormula expression");

    public static final Option<Integer> MAX_RECORDS =
            Options.key("max_records")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Maximum number of records to return, must be greater than 0");

    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Number of records per page, must be in range [1, 100]");

    public static final Option<String> SORT =
            Options.key("sort")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Sort definition JSON array, e.g. [{\"field\":\"Name\",\"direction\":\"asc\"}]");

    public static final Option<String> CELL_FORMAT =
            Options.key("cell_format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("cellFormat value, e.g. json or string");

    public static final Option<Boolean> RETURN_FIELDS_BY_FIELD_ID =
            Options.key("return_fields_by_field_id")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Return fields by field ID instead of field name");

    public static final Option<List<String>> RECORD_METADATA =
            Options.key("record_metadata")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Record metadata to return, e.g. [\"commentCount\"]");

    public static final Option<String> TIME_ZONE =
            Options.key("time_zone")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The time zone for cell values");

    public static final Option<String> USER_LOCALE =
            Options.key("user_locale")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The user locale for cell values");

    public static final Option<String> OFFSET =
            Options.key("offset")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Pagination offset returned by Airtable");
}
