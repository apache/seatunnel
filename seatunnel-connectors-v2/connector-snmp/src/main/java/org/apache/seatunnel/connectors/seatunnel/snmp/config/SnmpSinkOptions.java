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

package org.apache.seatunnel.connectors.seatunnel.snmp.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/** Configuration options for the SNMPv2c SET sink. */
public final class SnmpSinkOptions {

    public static final String CONNECTOR_IDENTITY = SnmpOptions.CONNECTOR_IDENTITY;

    public static final Option<String> HOST = SnmpOptions.HOST;
    public static final Option<Integer> PORT = SnmpOptions.PORT;
    public static final Option<String> COMMUNITY = SnmpOptions.COMMUNITY;
    public static final Option<Long> TIMEOUT_MILLIS = SnmpOptions.TIMEOUT_MILLIS;
    public static final Option<Integer> RETRIES = SnmpOptions.RETRIES;

    public static final Option<String> OID_FIELD =
            Options.key("oid_field")
                    .stringType()
                    .defaultValue("oid")
                    .withDescription("Input STRING field containing the numeric OID to set");

    public static final Option<String> VALUE_FIELD =
            Options.key("value_field")
                    .stringType()
                    .defaultValue("value")
                    .withDescription("Input STRING field containing the value to set");

    public static final Option<String> VALUE_TYPE_FIELD =
            Options.key("value_type_field")
                    .stringType()
                    .defaultValue("value_type")
                    .withDescription("Input STRING field containing the SMI value type");

    private SnmpSinkOptions() {}
}
