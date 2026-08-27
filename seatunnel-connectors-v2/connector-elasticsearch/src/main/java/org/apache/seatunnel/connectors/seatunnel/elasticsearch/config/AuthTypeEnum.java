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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.config;

public enum AuthTypeEnum {
    /** HTTP Basic Authentication using username and password */
    BASIC("basic"),

    /** Elasticsearch API Key authentication using api_key_id and api_key */
    API_KEY("api_key"),

    /** Elasticsearch API Key authentication using encoded api_key */
    API_KEY_ENCODED("api_key_encoded");

    private final String value;

    AuthTypeEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Get AuthTypeEnum from string value.
     *
     * @param value the string value
     * @return the corresponding AuthTypeEnum
     * @throws IllegalArgumentException if the value is not supported
     */
    public static AuthTypeEnum fromValue(String value) {
        for (AuthTypeEnum authType : values()) {
            if (authType.getValue().equals(value)) {
                return authType;
            }
        }
        throw new IllegalArgumentException("Unsupported auth type: " + value);
    }
}
