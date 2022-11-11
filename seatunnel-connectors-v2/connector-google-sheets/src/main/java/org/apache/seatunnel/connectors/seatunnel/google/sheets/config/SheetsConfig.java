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

package org.apache.seatunnel.connectors.seatunnel.google.sheets.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class SheetsConfig {

    public static final Option<String> SERVICE_ACCOUNT_KEY = Options.key("service_account_key")
            .stringType()
            .noDefaultValue()
            .withDescription("Google Sheets login service account key");
    public static final Option<String> SHEET_ID = Options.key("sheet_id")
            .stringType()
            .noDefaultValue()
            .withDescription("Google Sheets sheet id");
    public static final Option<String> SHEET_NAME = Options.key("sheet_name")
            .stringType()
            .noDefaultValue()
            .withDescription("Google Sheets sheet name that you want to import");
    public static final Option<String> RANGE = Options.key("range")
            .stringType()
            .noDefaultValue()
            .withDescription("Google Sheets sheet range that you want to import");
}
