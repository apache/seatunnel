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

package org.apache.seatunnel.connectors.seatunnel.notion.source.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;

public class NotionSourceConfig extends HttpConfig {
    public static final String AUTHORIZATION = "Authorization";
    public static final String BEARER = "Bearer";
    public static final String NOTION_VERSION = "Notion-Version";

    public static final Option<String> PASSWORD = Options.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("Notion login api key");
    public static final Option<String> VERSION = Options.key("version")
            .stringType()
            .noDefaultValue()
            .withDescription("The Notion API is versioned. API versions are named for the date the version is released");
}
