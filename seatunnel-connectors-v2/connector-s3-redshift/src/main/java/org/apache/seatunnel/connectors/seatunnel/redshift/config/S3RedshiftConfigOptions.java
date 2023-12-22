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

package org.apache.seatunnel.connectors.seatunnel.redshift.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3ConfigOptions;

public class S3RedshiftConfigOptions extends S3ConfigOptions {

    public static final Option<String> JDBC_URL =
            Options.key("jdbc_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redshift JDBC URL");

    public static final Option<String> JDBC_USER =
            Options.key("jdbc_user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redshift JDBC user");

    public static final Option<String> JDBC_PASSWORD =
            Options.key("jdbc_password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redshift JDBC password");

    public static final Option<String> EXECUTE_SQL =
            Options.key("execute_sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Redshift execute sql");
}
