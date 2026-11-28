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
package org.apache.seatunnel.connectors.seatunnel.campaignmonitor;

import java.util.regex.Pattern;

/**
 * Defines Campaign Monitor v3.3 read paths and their native identities.
 *
 * <p>Account resources are independent and client resources use the discovered client ID.
 */
enum CampaignMonitorResource {
    CLIENTS("clients", "/clients.json", "ClientID", Shape.ARRAY, false),
    ADMINS("admins", "/admins.json", "EmailAddress", Shape.ARRAY, false),
    CLIENT_DETAILS("client_details", "/clients/%s.json", "ClientID", Shape.OBJECT, true),
    SEGMENTS("segments", "/clients/%s/segments.json", "SegmentID", Shape.ARRAY, true),
    TEMPLATES("templates", "/clients/%s/templates.json", "TemplateID", Shape.ARRAY, true),
    PEOPLE("people", "/clients/%s/people.json", "EmailAddress", Shape.ARRAY, true),
    TAGS("tags", "/clients/%s/tags.json", "Name", Shape.ARRAY, true),
    SUBSCRIBER_LISTS("subscriber_lists", "/clients/%s/lists.json", "ListID", Shape.ARRAY, true),
    SUPPRESSION_LISTS(
            "suppression_lists",
            "/clients/%s/suppressionlist.json",
            "EmailAddress",
            Shape.PAGED,
            true),
    SENT_CAMPAIGNS("sent_campaigns", "/clients/%s/campaigns.json", "CampaignID", Shape.PAGED, true),
    DRAFT_CAMPAIGNS("draft_campaigns", "/clients/%s/drafts.json", "CampaignID", Shape.ARRAY, true),
    SCHEDULED_CAMPAIGNS(
            "scheduled_campaigns", "/clients/%s/scheduled.json", "CampaignID", Shape.ARRAY, true);

    private static final Pattern CLIENT_ID = Pattern.compile("[A-Za-z0-9_-]{1,160}");
    final String name;
    private final String route;
    final String identity;
    final Shape shape;
    final boolean scoped;

    CampaignMonitorResource(
            String name, String route, String identity, Shape shape, boolean scoped) {
        this.name = name;
        this.route = route;
        this.identity = identity;
        this.shape = shape;
        this.scoped = scoped;
    }

    String path(String clientId) {
        return "/api/v3.3" + (scoped ? String.format(route, clientId) : route);
    }

    static boolean clientKey(String id) {
        return id != null && CLIENT_ID.matcher(id).matches();
    }

    Failure failure(String reason) {
        return new Failure(name, reason);
    }

    /**
     * Describes the top-level response shape for a resource.
     *
     * <p>Only the two documented Results envelopes can advance a page.
     */
    enum Shape {
        ARRAY,
        OBJECT,
        PAGED
    }

    /**
     * Exposes a resource-specific diagnostic without provider details or credentials.
     *
     * <p>Raw responses and URLs stay outside exception messages.
     */
    static final class Failure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failure(String resource, String reason) {
            super("Campaign Monitor " + resource + ": " + reason);
        }
    }
}
