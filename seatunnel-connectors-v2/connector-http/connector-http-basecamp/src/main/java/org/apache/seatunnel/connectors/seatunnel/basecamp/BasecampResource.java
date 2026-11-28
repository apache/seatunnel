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
package org.apache.seatunnel.connectors.seatunnel.basecamp;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.util.Locale;

/**
 * Identifies each of the original Basecamp read resources.
 *
 * <p>Only recording feeds use the account-wide updated_at search.
 */
enum BasecampResource {
    PROJECTS(null),
    SCHEDULES(null),
    SCHEDULE_ENTRIES(null),
    TODOS("Todo"),
    MESSAGES("Message");

    final String label = name().toLowerCase(Locale.ROOT);
    final String recordingType;

    BasecampResource(String recordingType) {
        this.recordingType = recordingType;
    }

    String id(JsonNode row) {
        JsonNode value = row.isObject() ? row.get("id") : null;
        if (value == null || !value.isIntegralNumber()) {
            throw failure("missing identifier");
        }
        String id = value.asText();
        if (!validId(id)) {
            throw failure("invalid identifier");
        }
        return id;
    }

    static boolean validId(String id) {
        return id != null && id.matches("[1-9][0-9]{0,19}");
    }

    Failure failure(String reason) {
        return new Failure(label, reason);
    }

    /**
     * An operation failure that remains safe when propagated across worker threads.
     *
     * <p>Provider bodies, credentials and request query strings never enter its text.
     */
    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Failure(String operation, String reason) {
            super("Basecamp " + operation + ": " + reason);
        }
    }
}
