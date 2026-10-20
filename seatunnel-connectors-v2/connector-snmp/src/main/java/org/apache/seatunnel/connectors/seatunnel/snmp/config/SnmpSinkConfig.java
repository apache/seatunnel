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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/** Validated runtime configuration for the SNMPv2c SET sink. */
public final class SnmpSinkConfig implements Serializable, SnmpTargetConfig {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final String community;
    private final long timeoutMillis;
    private final int retries;
    private final String oidField;
    private final String valueField;
    private final String valueTypeField;

    public SnmpSinkConfig(ReadonlyConfig config) {
        String configuredHost = config.get(SnmpSinkOptions.HOST);
        if (isBlank(configuredHost)) {
            throw new IllegalArgumentException("SNMP sink host must not be blank");
        }
        this.host = configuredHost.trim();
        this.port = config.get(SnmpSinkOptions.PORT);
        this.community = config.get(SnmpSinkOptions.COMMUNITY);
        this.timeoutMillis = config.get(SnmpSinkOptions.TIMEOUT_MILLIS);
        this.retries = config.get(SnmpSinkOptions.RETRIES);
        this.oidField = requireField(config.get(SnmpSinkOptions.OID_FIELD), "oid_field");
        this.valueField = requireField(config.get(SnmpSinkOptions.VALUE_FIELD), "value_field");
        this.valueTypeField =
                requireField(config.get(SnmpSinkOptions.VALUE_TYPE_FIELD), "value_type_field");
        validateTarget();
        validateDistinctFields();
    }

    private void validateTarget() {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("SNMP sink port must be between 1 and 65535");
        }
        if (isBlank(community)) {
            throw new IllegalArgumentException("SNMP sink community must not be blank");
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("SNMP sink timeout_millis must be greater than 0");
        }
        if (retries < 0) {
            throw new IllegalArgumentException("SNMP sink retries must not be negative");
        }
    }

    private static String requireField(String configuredField, String optionName) {
        if (isBlank(configuredField)) {
            throw new IllegalArgumentException("SNMP sink " + optionName + " must not be blank");
        }
        return configuredField.trim();
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private void validateDistinctFields() {
        Set<String> fields = new HashSet<>();
        fields.add(oidField);
        fields.add(valueField);
        fields.add(valueTypeField);
        if (fields.size() != 3) {
            throw new IllegalArgumentException(
                    "SNMP sink oid_field, value_field, and value_type_field must be distinct");
        }
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getCommunity() {
        return community;
    }

    @Override
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    @Override
    public int getRetries() {
        return retries;
    }

    public String getOidField() {
        return oidField;
    }

    public String getValueField() {
        return valueField;
    }

    public String getValueTypeField() {
        return valueTypeField;
    }
}
