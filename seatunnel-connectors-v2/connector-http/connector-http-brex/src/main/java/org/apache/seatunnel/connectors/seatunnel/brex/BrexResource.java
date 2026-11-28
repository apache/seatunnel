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
package org.apache.seatunnel.connectors.seatunnel.brex;

import java.util.Locale;

/**
 * Describes the six read-only Brex collections and their paging limits.
 *
 * <p>Native record identifiers remain separate from the optional incremental timestamp.
 */
enum BrexResource {
    TRANSACTIONS("/v2/transactions/card/primary", "id", 1000),
    USERS("/v2/users", "id", 1000),
    DEPARTMENTS("/v2/departments", "id", 1000),
    VENDORS("/v1/vendors", "id", 1000),
    EXPENSES("/v1/expenses", "id", 100),
    BUDGETS("/v2/budgets", "budget_id", 1000);

    final String path;
    final String identityField;
    final int limit;
    final String name = name().toLowerCase(Locale.ROOT);

    BrexResource(String path, String identityField, int limit) {
        this.path = path;
        this.identityField = identityField;
        this.limit = limit;
    }

    boolean dated() {
        return this == TRANSACTIONS || this == EXPENSES;
    }

    Failure failure(String reason) {
        return new Failure(name, reason);
    }

    /**
     * Reports a collection and a vetted diagnostic.
     *
     * <p>Third-party response text, cursors and request credentials are excluded.
     */
    static final class Failure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failure(String resource, String reason) {
            super("Brex " + resource + ": " + reason);
        }
    }
}
