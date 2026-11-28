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
package org.apache.seatunnel.connectors.seatunnel.auth0;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.util.Locale;

/**
 * Auth0 read populations and their provider identities.
 *
 * <p>Organization and member IDs in child paths are validated separately from record data.
 */
enum Auth0Resource {
    CLIENTS("client_id", null),
    USERS("user_id", null),
    ORGANIZATIONS("id", "organizations"),
    ORGANIZATION_MEMBERS("user_id", "members"),
    ORGANIZATION_MEMBER_ROLES("id", null);

    final String label = name().toLowerCase(Locale.ROOT);
    final String field;
    final String envelope;

    Auth0Resource(String field, String envelope) {
        this.field = field;
        this.envelope = envelope;
    }

    String id(JsonNode row) {
        String id = text(row.get(field), 1024);
        if (id == null || id.equals(".") || id.equals("..")) {
            throw failure("record is missing a valid " + field);
        }
        return id;
    }

    static String text(JsonNode value, int maximum) {
        if (value == null || !value.isTextual()) {
            return null;
        }
        String text = value.textValue();
        if (text.isBlank()
                || text.length() > maximum
                || text.codePoints()
                        .anyMatch(
                                codePoint ->
                                        Character.isISOControl(codePoint)
                                                || codePoint >= Character.MIN_SURROGATE
                                                        && codePoint <= Character.MAX_SURROGATE)) {
            return null;
        }
        return text;
    }

    Failure failure(String reason) {
        return new Failure(label, reason);
    }

    /**
     * Carries controlled operation context without including remote content or credentials.
     *
     * <p>The reader preserves these diagnosable failures across its network worker boundary.
     */
    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Failure(String operation, String reason) {
            super("Auth0 " + operation + ": " + reason);
        }
    }
}
