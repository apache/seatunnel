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

package org.apache.seatunnel.connectors.seatunnel.myhours.source.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;

public class MyHoursSourceConfig extends HttpConfig {
    public static final String POST = "POST";
    public static final String GRANT_TYPE = "grantType";
    public static final String CLIENT_ID = "clientId";
    public static final String API = "api";
    public static final String AUTHORIZATION = "Authorization";
    public static final String ACCESS_TOKEN = "accessToken";
    public static final String ACCESS_TOKEN_PREFIX = "Bearer";
    public static final String AUTHORIZATION_URL = "https://api2.myhours.com/api/tokens/login";
    public static final Option<String> EMAIL = Options.key("email")
            .stringType()
            .noDefaultValue()
            .withDescription("My hours login email address");
    public static final Option<String> PASSWORD = Options.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("My hours login password");
}
