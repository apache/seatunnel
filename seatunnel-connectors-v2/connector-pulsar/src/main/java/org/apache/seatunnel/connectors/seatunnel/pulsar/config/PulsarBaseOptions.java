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

package org.apache.seatunnel.connectors.seatunnel.pulsar.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

public class PulsarBaseOptions extends ConnectorCommonOptions {

    public static final String IDENTIFIER = "Pulsar";

    public static final Option<String> TOPIC =
            Options.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("pulsar topic name.");

    public static final Option<String> CLIENT_SERVICE_URL =
            Options.key("client.service-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Service URL provider for Pulsar service");

    public static final Option<String> ADMIN_SERVICE_URL =
            Options.key("admin.service-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Pulsar service HTTP URL for the admin endpoint. For example, http://my-broker.example.com:8080, or https://my-broker.example.com:8443 for TLS.");

    public static final Option<String> AUTH_PLUGIN_CLASS =
            Options.key("auth.plugin-class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the authentication plugin");

    public static final Option<String> AUTH_PARAMS =
            Options.key("auth.params")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Parameters for the authentication plugin. For example, key1:val1,key2:val2");

    /** The default data format is JSON */
    public static final String DEFAULT_FORMAT = "json";

    public static final String TEXT_FORMAT = "text";

    /** The default field delimiter is “,” */
    public static final String DEFAULT_FIELD_DELIMITER = ",";

    public static final Option<String> FORMAT =
            Options.key("format")
                    .stringType()
                    .defaultValue(DEFAULT_FORMAT)
                    .withDescription(
                            "Data format. The default format is json. Optional text format. The default field separator is \", \". "
                                    + "If you customize the delimiter, add the \"field_delimiter\" option.");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(DEFAULT_FIELD_DELIMITER)
                    .withDescription(
                            "Customize the field delimiter for data format.The default field_delimiter is ',' ");
}
