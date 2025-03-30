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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.SchemaFormat;

public class Config {

    /** The default field delimiter is “,” */
    public static final String DEFAULT_FIELD_DELIMITER = ",";

    public static final String CONNECTOR_IDENTITY = "Rocketmq";

    public static final Option<String> NAME_SRV_ADDR =
            Options.key("name.srv.addr")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RocketMq name server configuration center address.");

    public static final Option<Boolean> ACL_ENABLED =
            Options.key("acl.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, access control is enabled, and access key and secret key need to be "
                                    + "configured.");

    public static final Option<String> ACCESS_KEY =
            Options.key("access.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("When ACL_ENABLED is true, access key cannot be empty.");

    public static final Option<String> SECRET_KEY =
            Options.key("secret.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("When ACL_ENABLED is true, secret key cannot be empty.");

    public static final Option<String> FORMAT =
            Options.key("format")
                    .stringType()
                    .defaultValue(SchemaFormat.JSON.getName())
                    .withDescription(
                            "Data format. The default format is json. Optional text format. The default field separator is \", \". "
                                    + "If you customize the delimiter, add the \"field.delimiter\" option.");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field.delimiter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Customize the field delimiter for data format.");
}
