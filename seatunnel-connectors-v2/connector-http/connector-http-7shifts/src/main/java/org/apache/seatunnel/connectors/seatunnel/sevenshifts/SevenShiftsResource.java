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

package org.apache.seatunnel.connectors.seatunnel.sevenshifts;

import java.util.Locale;

/** Endpoint-specific parent, shape, paging and modification-filter rules. */
enum SevenShiftsResource {
    COMPANIES(Scope.ROOT, Shape.LIST, Modification.DAY),
    LOCATIONS(Scope.COMPANY, Shape.LIST, Modification.DAY),
    DEPARTMENTS(Scope.COMPANY, Shape.LIST, Modification.DAY),
    ROLES(Scope.COMPANY, Shape.LIST, Modification.DAY),
    USERS(Scope.COMPANY, Shape.LIST, Modification.DAY),
    WAGES(Scope.USER, Shape.OBJECT, Modification.NONE),
    ASSIGNMENTS(Scope.USER, Shape.OBJECT, Modification.NONE),
    LOCATION_ASSIGNMENTS(Scope.USER, Shape.LIST, Modification.NONE),
    DEPARTMENT_ASSIGNMENTS(Scope.USER, Shape.LIST, Modification.NONE),
    ROLE_ASSIGNMENTS(Scope.USER, Shape.LIST, Modification.NONE),
    TIME_PUNCHES(Scope.COMPANY, Shape.LIST, Modification.INSTANT),
    SHIFTS(Scope.COMPANY, Shape.LIST, Modification.INSTANT);

    enum Scope {
        ROOT,
        COMPANY,
        USER
    }

    enum Shape {
        LIST,
        OBJECT
    }

    enum Modification {
        NONE,
        DAY,
        INSTANT
    }

    final Scope scope;
    final Shape shape;
    final Modification modification;
    final String apiName;

    SevenShiftsResource(Scope scope, Shape shape, Modification modification) {
        this.scope = scope;
        this.shape = shape;
        this.modification = modification;
        apiName = name().toLowerCase(Locale.ROOT);
    }

    static SevenShiftsResource parse(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw SevenShiftsConfig.invalid("resource is unsupported");
        }
    }

    String route(Long companyId, Long userId) {
        if (scope == Scope.ROOT) {
            return "/v2/companies";
        }
        String prefix = "/v2/company/" + companyId;
        if (scope == Scope.USER) {
            prefix += "/users/" + userId;
        }
        return prefix + "/" + apiName;
    }
}
