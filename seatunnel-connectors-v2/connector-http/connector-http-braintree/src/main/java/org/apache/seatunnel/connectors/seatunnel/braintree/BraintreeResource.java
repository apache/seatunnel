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
package org.apache.seatunnel.connectors.seatunnel.braintree;

/**
 * Native XML routes supported by the Braintree source.
 *
 * <p>Customers, transactions and subscriptions fetch IDs first. Disputes and merchant accounts use
 * the gateway's page metadata, while discounts and plans return one collection.
 */
enum BraintreeResource {
    MERCHANT_ACCOUNTS(
            "merchant_accounts", "merchant-accounts", "merchant-account", Mode.PAGED, null, 0),
    CUSTOMERS("customers", "customers", "customer", Mode.IDS, "created_at", 10000),
    DISCOUNTS("discounts", "discounts", "discount", Mode.LIST, null, 0),
    PLANS("plans", "plans", "plan", Mode.LIST, null, 0),
    TRANSACTIONS(
            "transactions",
            "credit-card-transactions",
            "transaction",
            Mode.IDS,
            "created_at",
            50000),
    SUBSCRIPTIONS("subscriptions", "subscriptions", "subscription", Mode.IDS, "created_at", 10000),
    DISPUTES("disputes", "disputes", "dispute", Mode.PAGED, "received_date", 10000);

    final String name;
    final String root;
    final String element;
    final Mode mode;
    final String cursor;
    final int providerLimit;

    BraintreeResource(
            String name, String root, String element, Mode mode, String cursor, int providerLimit) {
        this.name = name;
        this.root = root;
        this.element = element;
        this.mode = mode;
        this.cursor = cursor;
        this.providerLimit = providerLimit;
    }

    boolean dated() {
        return cursor != null;
    }

    boolean daily() {
        return this == DISPUTES;
    }

    BraintreeFailure failure(String reason) {
        return new BraintreeFailure(name + " " + reason);
    }

    /**
     * Chooses the request protocol used to advance a collection.
     *
     * <p>The XML envelopes and progress checks differ by mode.
     */
    enum Mode {
        LIST,
        PAGED,
        IDS
    }
}
