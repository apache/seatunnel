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
package org.apache.seatunnel.connectors.seatunnel.callrail;

/**
 * The four documented account list operations and their dates and field selections.
 *
 * <p>Only calls and conversations declare date filtering. User and company creation times are
 * evaluated after each ordinary numbered page is fetched.
 */
enum CallRailResource {
    CALLS("calls", "calls", "start_time", true, Projection.CALLS),
    CONVERSATIONS(
            "conversations", "text-messages", "last_message_at", true, Projection.CONVERSATIONS),
    USERS("users", "users", "created_at", false, null),
    COMPANIES("companies", "companies", "created_at", false, null);

    final String id;
    final String path;
    final String time;
    final boolean dateFiltered;
    final String fields;

    CallRailResource(String id, String path, String time, boolean dateFiltered, String fields) {
        this.id = id;
        this.path = path;
        this.time = time;
        this.dateFiltered = dateFiltered;
        this.fields = fields;
    }

    static CallRailResource from(String text) {
        for (CallRailResource item : values()) {
            if (item.id.equals(text)) {
                return item;
            }
        }
        throw CallRailConfig.invalid("resources", "unknown resource name");
    }

    boolean relative(CallRailConfig config) {
        return this == CALLS && config.relativeCalls;
    }

    CallRailReadException failure(int page, String message) {
        return new CallRailReadException(id + " page " + page, message);
    }

    /**
     * Supported field selections for the two communication records.
     *
     * <p>Fields beyond the documented default payload include attribution, caller formatting and
     * conversation message summaries.
     */
    private static final class Projection {
        private static final String CALLS =
                String.join(
                        ",",
                        "agent_email",
                        "call_highlights",
                        "call_type",
                        "campaign",
                        "company_id",
                        "company_name",
                        "company_time_zone",
                        "created_at",
                        "device_type",
                        "fbclid",
                        "first_call",
                        "formatted_business_phone_number",
                        "formatted_call_type",
                        "formatted_customer_location",
                        "formatted_customer_name",
                        "formatted_customer_name_or_phone_number",
                        "formatted_customer_phone_number",
                        "formatted_duration",
                        "formatted_tracking_phone_number",
                        "formatted_tracking_source",
                        "formatted_value",
                        "ga",
                        "gclid",
                        "good_lead_call_id",
                        "good_lead_call_time",
                        "keypad_entries",
                        "keywords",
                        "keywords_spotted",
                        "landing_page_url",
                        "last_requested_url",
                        "lead_status",
                        "medium",
                        "milestones",
                        "msclkid",
                        "note",
                        "prior_calls",
                        "referrer_domain",
                        "referring_url",
                        "source",
                        "source_name",
                        "speaker_percent",
                        "tags",
                        "timeline_url",
                        "total_calls",
                        "tracker_id",
                        "utm_campaign",
                        "utm_content",
                        "utm_medium",
                        "utm_source",
                        "utm_term",
                        "value",
                        "waveforms");

        private static final String CONVERSATIONS =
                String.join(
                        ",",
                        "company_id",
                        "company_time_zone",
                        "current_tracker_id",
                        "current_tracking_number",
                        "customer_name",
                        "customer_phone_number",
                        "formatted_current_tracking_number",
                        "formatted_customer_name",
                        "formatted_customer_phone_number",
                        "formatted_initial_tracking_number",
                        "id",
                        "initial_tracker_id",
                        "initial_tracking_number",
                        "last_message_at",
                        "recent_messages",
                        "state");

        private Projection() {}
    }
}
