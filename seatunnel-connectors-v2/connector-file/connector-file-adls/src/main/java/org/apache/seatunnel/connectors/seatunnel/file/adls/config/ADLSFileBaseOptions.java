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
package org.apache.seatunnel.connectors.seatunnel.file.adls.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;

import java.util.Map;

public class ADLSFileBaseOptions extends FileBaseSourceOptions {
    public enum AuthType {
        SHARED_KEY,
        OAUTH_CLIENT_CREDENTIALS
    }

    public static final Option<String> ACCOUNT_NAME =
            Options.key("account_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ADLS Gen2 storage account name");
    public static final Option<String> CONTAINER =
            Options.key("container")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ADLS Gen2 filesystem/container");
    public static final Option<String> ENDPOINT_SUFFIX =
            Options.key("endpoint_suffix")
                    .stringType()
                    .defaultValue("dfs.core.windows.net")
                    .withDescription("ADLS Gen2 endpoint suffix");
    public static final Option<AuthType> AUTH_TYPE =
            Options.key("auth_type")
                    .enumType(AuthType.class)
                    .defaultValue(AuthType.SHARED_KEY)
                    .withDescription("ADLS authentication mode");
    public static final Option<String> ACCOUNT_KEY =
            Options.key("account_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Storage account key");
    public static final Option<String> TENANT_ID =
            Options.key("tenant_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Microsoft Entra tenant ID");
    public static final Option<String> CLIENT_ID =
            Options.key("client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Microsoft Entra client ID");
    public static final Option<String> CLIENT_SECRET =
            Options.key("client_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Microsoft Entra client secret");
    public static final Option<String> AUTHORITY_HOST =
            Options.key("authority_host")
                    .stringType()
                    .defaultValue("https://login.microsoftonline.com")
                    .withDescription("Microsoft Entra authority host");
    public static final Option<Map<String, String>> HADOOP_PROPERTIES =
            Options.key("hadoop_adls_properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Additional Hadoop ABFS properties");
}
