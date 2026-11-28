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
package org.apache.seatunnel.connectors.seatunnel.apptivo;

/**
 * Identifies the Apptivo DAO action and row identity for each resource.
 *
 * <p>Case settings are a single object with no server pagination or case-instance ID.
 */
enum ApptivoResource {
    CUSTOMERS("customers", "getAll", "customerId"),
    CONTACTS("contacts", "getAll", "contactId"),
    CASES("cases", "getConfigData", null),
    LEADS("leads", "getAll", "id"),
    OPPORTUNITIES("opportunities", "getAll", "opportunityId");

    final String label;
    final String action;
    final String id;

    ApptivoResource(String label, String action, String id) {
        this.label = label;
        this.action = action;
        this.id = id;
    }

    boolean singleton() {
        return id == null;
    }

    Failure failure(String reason) {
        return new Failure("Apptivo " + label + ": " + reason);
    }

    /**
     * Exposes a fixed resource label and safe diagnostic.
     *
     * <p>Request targets, credentials and raw provider response text never reach the cause chain.
     */
    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Failure(String message) {
            super(message);
        }
    }
}
