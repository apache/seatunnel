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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import java.io.Serializable;

public class AmazonDynamoDBBaseOptions extends ConnectorCommonOptions implements Serializable {
    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("url to read to Amazon DynamoDB");
    public static final Option<String> REGION =
            Options.key("region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The region of Amazon DynamoDB");
    public static final Option<String> ACCESS_KEY_ID =
            Options.key("access_key_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The access id of Amazon DynamoDB");
    public static final Option<String> SECRET_ACCESS_KEY =
            Options.key("secret_access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The access secret key of Amazon DynamoDB");
    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table of Amazon DynamoDB");
}
