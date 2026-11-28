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
package org.apache.seatunnel.connectors.seatunnel.avni;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.util.Locale;

/**
 * The four bounded Avni integration API collections.
 *
 * <p>Each route is constant and returns a provider ID independently from its related records.
 */
enum AvniResource {
    SUBJECTS("/api/subjects"),
    PROGRAM_ENROLMENTS("/api/programEnrolments"),
    PROGRAM_ENCOUNTERS("/api/programEncounters"),
    ENCOUNTERS("/api/encounters");

    final String label = name().toLowerCase(Locale.ROOT);
    final String path;

    AvniResource(String path) {
        this.path = path;
    }

    static String text(JsonNode value, int maximum) {
        if (value == null || !value.isTextual()) {
            return null;
        }
        String text = value.textValue();
        return text.isBlank()
                        || text.length() > maximum
                        || text.codePoints().anyMatch(Character::isISOControl)
                ? null
                : text;
    }

    Failure failure(String reason) {
        return new Failure(label, reason);
    }

    /**
     * Preserves safe operation context across the reader's worker boundary.
     *
     * <p>Service bodies, credentials and personal record fields never enter these messages.
     */
    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Failure(String operation, String reason) {
            super("Avni " + operation + ": " + reason);
        }
    }
}
