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
package org.apache.seatunnel.connectors.seatunnel.calcom;

/**
 * Describes the Cal.com v2 resources and their native record identifiers.
 *
 * <p>The reader keeps a separate page position for each selected resource.
 */
enum CalComResource {
    EVENT_TYPES("event_types", "/v2/event-types", "id"),
    MY_PROFILE("my_profile", "/v2/me", "id"),
    SCHEDULES("schedules", "/v2/schedules", "id"),
    CALENDARS("calendars", "/v2/calendars", "externalId"),
    BOOKINGS("bookings", "/v2/bookings", "id"),
    CONFERENCING("conferencing", "/v2/conferencing", "id");

    final String name;
    final String path;
    final String identity;

    CalComResource(String name, String path, String identity) {
        this.name = name;
        this.path = path;
        this.identity = identity;
    }

    Failure failure(String reason) {
        return new Failure(name, reason);
    }

    /**
     * Reports a read failure with a resource and a connector-defined diagnostic.
     *
     * <p>Provider bodies, credentials and cursors are excluded.
     */
    static final class Failure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failure(String name, String reason) {
            super("Cal.com " + name + ": " + reason);
        }
    }
}
