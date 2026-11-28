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

package org.apache.seatunnel.connectors.seatunnel.acuityscheduling;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.Locale;

/** Seven list endpoints and their documented query shapes. */
enum AcuityResource {
    APPOINTMENTS("appointments", true),
    CALENDARS("calendars", false),
    CLIENTS("clients", false),
    APPOINTMENT_TYPES("appointment-types", false),
    BLOCKS("blocks", true),
    LABELS("labels", false),
    FORMS("forms", false);

    final String name;
    final boolean dated;

    AcuityResource(String name, boolean dated) {
        this.name = name;
        this.dated = dated;
    }

    static AcuityResource parse(String value) {
        if (value != null) {
            for (AcuityResource resource : values()) {
                if (resource.name.equals(value.toLowerCase(Locale.ROOT))) {
                    return resource;
                }
            }
        }
        throw AcuityConfig.invalid("resources contains an unsupported name");
    }

    String request(AcuityProgress.Window window, AcuityConfig config) {
        String path = "/api/v1/" + name;
        if (!dated) {
            return path;
        }
        StringBuilder target =
                new StringBuilder(path)
                        .append("?max=")
                        .append(config.dateLimit)
                        .append("&minDate=")
                        .append(window.start)
                        .append("&maxDate=")
                        .append(window.end);
        if (this == APPOINTMENTS) {
            target.append("&showall=true");
        }
        if (config.calendarId != null) {
            target.append("&calendarID=").append(config.calendarId);
        }
        return target.toString();
    }

    String identity(JsonNode object) throws IOException {
        if (!object.isObject()) {
            throw failure("expected a response array of JSON objects");
        }
        JsonNode id = object.get("id");
        if (id != null
                && (id.isIntegralNumber() || (id.isTextual() && !id.textValue().trim().isEmpty()))
                && id.asText().length() <= 512) {
            return id.asText();
        }
        if (this == CLIENTS && (id == null || id.isNull())) {
            JsonNode email = object.get("email");
            if (email != null && email.isTextual() && !email.textValue().trim().isEmpty()) {
                return "email:" + AcuityConfig.digest(email.textValue());
            }
            return "row:" + AcuityConfig.digest(object.toString());
        }
        throw failure("missing or invalid provider record ID");
    }

    IOException failure(String reason) {
        return new IOException("Acuity Scheduling source " + name + ": " + reason);
    }
}
