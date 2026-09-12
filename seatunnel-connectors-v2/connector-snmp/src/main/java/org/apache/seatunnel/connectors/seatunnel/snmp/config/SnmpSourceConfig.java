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

import org.snmp4j.smi.OID;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/** Validated runtime configuration for the SNMP source connector. */
public final class SnmpSourceConfig implements Serializable, SnmpTargetConfig {

    private static final long serialVersionUID = 1L;

    private static final Pattern NUMERIC_OID = Pattern.compile("^\\.?[0-9]+(\\.[0-9]+)+$");

    private final String host;
    private final int port;
    private final String community;
    private final List<OID> oids;
    private final long timeoutMillis;
    private final int retries;
    private final long pollIntervalMillis;

    /** Creates an SNMP source configuration from connector options. */
    public SnmpSourceConfig(ReadonlyConfig config) {
        String configuredHost = config.get(SnmpSourceOptions.HOST);
        if (isBlank(configuredHost)) {
            throw new IllegalArgumentException("SNMP source host must not be blank");
        }
        this.host = configuredHost.trim();
        this.port = config.get(SnmpSourceOptions.PORT);
        this.community = config.get(SnmpSourceOptions.COMMUNITY);
        this.oids = parseOids(config.get(SnmpSourceOptions.OIDS));
        this.timeoutMillis = config.get(SnmpSourceOptions.TIMEOUT_MILLIS);
        this.retries = config.get(SnmpSourceOptions.RETRIES);
        this.pollIntervalMillis = config.get(SnmpSourceOptions.POLL_INTERVAL_MILLIS);
        validate();
    }

    private void validate() {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("SNMP source port must be between 1 and 65535");
        }
        if (isBlank(community)) {
            throw new IllegalArgumentException("SNMP source community must not be blank");
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("SNMP source timeout_millis must be greater than 0");
        }
        if (retries < 0) {
            throw new IllegalArgumentException("SNMP source retries must not be negative");
        }
        if (pollIntervalMillis <= 0) {
            throw new IllegalArgumentException(
                    "SNMP source poll_interval_millis must be greater than 0");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
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

    private static List<OID> parseOids(List<String> configuredOids) {
        if (configuredOids == null || configuredOids.isEmpty()) {
            throw new IllegalArgumentException("SNMP source oids must not be empty");
        }

        List<OID> parsed = new ArrayList<>(configuredOids.size());
        Set<String> unique = new LinkedHashSet<>();
        for (String configuredOid : configuredOids) {
            String value = configuredOid == null ? null : configuredOid.trim();
            if (value == null || !NUMERIC_OID.matcher(value).matches()) {
                throw new IllegalArgumentException(
                        "SNMP source oids must contain only numeric OIDs: " + configuredOid);
            }
            if (value.charAt(0) == '.') {
                value = value.substring(1);
            }
            OID oid;
            try {
                oid = new OID(value);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Invalid SNMP source OID: " + value, e);
            }
            String normalized = oid.toString();
            if (!unique.add(normalized)) {
                throw new IllegalArgumentException("Duplicate SNMP source OID: " + normalized);
            }
            parsed.add(oid);
        }
        return Collections.unmodifiableList(parsed);
    }

    public List<OID> getOids() {
        return oids;
    }

    @Override
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    @Override
    public int getRetries() {
        return retries;
    }

    public long getPollIntervalMillis() {
        return pollIntervalMillis;
    }
}
