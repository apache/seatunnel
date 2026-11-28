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

package org.apache.seatunnel.connectors.seatunnel.brevo;

/**
 * Defines the Brevo exports and their request and payload boundaries.
 *
 * <p>Endpoint paging and projection are specified per resource because Brevo uses different listing
 * envelopes, date contracts and page sizes across product areas.
 */
enum BrevoResource {
    CONTACTS(
            "contacts",
            "/contacts",
            "contacts",
            "id",
            1000,
            TimeFilter.MODIFIED_SINCE,
            "modifiedAt"),
    CONTACT_ATTRIBUTES("contacts_attributes", "/contacts/attributes", "attributes", "name"),
    FOLDERS("contacts_folders", "/contacts/folders", "folders", "id", 50, TimeFilter.NONE, null),
    FOLDER_LISTS(
            "contacts_folders_lists",
            "/contacts/folders/{parent}/lists",
            "lists",
            "id",
            50,
            TimeFilter.NONE,
            null,
            FOLDERS,
            "folderId"),
    SEGMENTS(
            "contacts_segments", "/contacts/segments", "segments", "id", 50, TimeFilter.NONE, null),
    LISTS("contacts_lists", "/contacts/lists", "lists", "id", 50, TimeFilter.NONE, null),
    LIST_CONTACTS(
            "contacts_lists_contacts",
            "/contacts/lists/{parent}/contacts",
            "contacts",
            "id",
            500,
            TimeFilter.MODIFIED_SINCE,
            "modifiedAt",
            LISTS,
            "listId"),
    SENDERS("senders", "/senders", "senders", "id"),
    COMPANIES(
            "companies",
            "/companies",
            "items",
            "id",
            500,
            TimeFilter.MODIFIED_SINCE,
            "last_updated_at"),
    COMPANY_ATTRIBUTES("companies_attributes", "/crm/attributes/companies", null, "internalName"),
    PIPELINE_STAGES("crm_pipeline_stages", "/crm/pipeline/details", "stages", "id"),
    PIPELINES("crm_pipeline_details_all", "/crm/pipeline/details/all", null, "pipeline"),
    DEAL_ATTRIBUTES("crm_attributes_deals", "/crm/attributes/deals", null, "internalName"),
    DEALS(
            "crm_deals",
            "/crm/deals",
            "items",
            "id",
            500,
            TimeFilter.MODIFIED_SINCE,
            "last_updated_date"),
    TASK_TYPES("crm_tasktypes", "/crm/tasktypes", null, "id"),
    TASKS("crm_tasks", "/crm/tasks", "items", "id", 50, TimeFilter.LOCAL, "updatedAt"),
    NOTES("crm_notes", "/crm/notes", null, "id", 50, TimeFilter.LOCAL, "updatedAt"),
    DOMAINS("domains", "/senders/domains", "domains", "id"),
    WEBHOOKS("webhooks", "/webhooks", "webhooks", "id"),
    ACCOUNT("account", "/account", null, "organization_id"),
    INVITED_USERS("organization_invited_users", "/organization/invited/users", "users", "email"),
    EMAIL_CAMPAIGNS(
            "emailCampaigns",
            "/emailCampaigns",
            "campaigns",
            "id",
            100,
            TimeFilter.LOCAL,
            "modifiedAt"),
    SMS_CAMPAIGNS(
            "smsCampaigns",
            "/smsCampaigns",
            "campaigns",
            "id",
            1000,
            TimeFilter.LOCAL,
            "modifiedAt"),
    INBOUND_EVENTS(
            "inbound_email_events",
            "/inbound/events",
            "events",
            "uuid",
            500,
            TimeFilter.INBOUND,
            "date"),
    TRANSACTIONAL_EVENTS(
            "transactional_email_events",
            "/smtp/statistics/events",
            "events",
            "messageId",
            5000,
            TimeFilter.TRANSACTIONAL,
            "date"),
    PROCESSES("processes", "/processes", "processes", "id", 50, TimeFilter.NONE, null),
    PRODUCTS(
            "products",
            "/products",
            "products",
            "id",
            1000,
            TimeFilter.MODIFIED_SINCE,
            "modifiedAt"),
    CATEGORIES(
            "categories",
            "/categories",
            "categories",
            "id",
            100,
            TimeFilter.MODIFIED_SINCE,
            "modifiedAt"),
    TEMPLATES("email_templates", "/smtp/templates", "templates", "id", 1000, TimeFilter.NONE, null);

    final String id;
    final String path;
    final String array;
    final String identity;
    final int maxPageSize;
    final TimeFilter filter;
    final String modifiedField;
    final BrevoResource parent;
    final String parentField;

    BrevoResource(String id, String path, String array, String identity) {
        this(id, path, array, identity, 0, TimeFilter.NONE, null);
    }

    BrevoResource(
            String id,
            String path,
            String array,
            String identity,
            int maxPageSize,
            TimeFilter filter,
            String modifiedField) {
        this(id, path, array, identity, maxPageSize, filter, modifiedField, null, null);
    }

    BrevoResource(
            String id,
            String path,
            String array,
            String identity,
            int maxPageSize,
            TimeFilter filter,
            String modifiedField,
            BrevoResource parent,
            String parentField) {
        this.id = id;
        this.path = path;
        this.array = array;
        this.identity = identity;
        this.maxPageSize = maxPageSize;
        this.filter = filter;
        this.modifiedField = modifiedField;
        this.parent = parent;
        this.parentField = parentField;
    }

    static BrevoResource parse(String name) {
        if ("campaigns".equals(name)) {
            return EMAIL_CAMPAIGNS;
        }
        if ("templates".equals(name)) {
            return TEMPLATES;
        }
        for (BrevoResource resource : values()) {
            if (resource.id.equals(name)) {
                return resource;
            }
        }
        throw BrevoConfig.invalid("resource must name one of the documented Brevo exports");
    }

    int firstPosition() {
        return this == COMPANIES ? 1 : 0;
    }

    int nextPosition(int position, int rawSize) {
        return position + (this == COMPANIES ? 1 : rawSize);
    }

    boolean usesNestedUpdatedTime() {
        return this == COMPANIES || this == DEALS;
    }

    /**
     * Determines whether a local filter can safely be pushed to Brevo.
     *
     * <p>Only documented modification-time filters are sent. The event endpoints have separate date
     * representations, and campaign and CRM filters do not guarantee modified-time semantics.
     */
    enum TimeFilter {
        NONE,
        MODIFIED_SINCE,
        LOCAL,
        INBOUND,
        TRANSACTIONAL;

        boolean windowed() {
            return this == INBOUND || this == TRANSACTIONAL;
        }
    }
}
