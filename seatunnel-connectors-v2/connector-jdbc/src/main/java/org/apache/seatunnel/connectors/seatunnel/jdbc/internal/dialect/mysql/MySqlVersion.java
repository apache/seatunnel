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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

public enum MySqlVersion {
    V_5_5("5.5"),
    V_5_6("5.6"),
    V_5_7("5.7"),
    V_8("8.0"),
    V_8_1("8.1"),
    V_8_2("8.2"),
    V_8_3("8.3"),
    V_8_4("8.4");

    private final String versionPrefix;

    MySqlVersion(String versionPrefix) {
        this.versionPrefix = versionPrefix;
    }

    public static MySqlVersion parse(String version) {
        if (version != null) {
            for (MySqlVersion mySqlVersion : values()) {
                if (version.startsWith(mySqlVersion.versionPrefix)) {
                    return mySqlVersion;
                }
            }
        }
        throw new UnsupportedOperationException("Unsupported MySQL version: " + version);
    }

    public boolean isBefore(MySqlVersion version) {
        return this.compareTo(version) < 0;
    }

    public boolean isAtOrBefore(MySqlVersion version) {
        return this.compareTo(version) <= 0;
    }

    public boolean isAfter(MySqlVersion version) {
        return this.compareTo(version) > 0;
    }

    public boolean isAtOrAfter(MySqlVersion version) {
        return this.compareTo(version) >= 0;
    }
}
