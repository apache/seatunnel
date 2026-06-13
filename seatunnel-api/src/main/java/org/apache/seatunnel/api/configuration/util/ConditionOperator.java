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

package org.apache.seatunnel.api.configuration.util;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ConditionOperator {

    // ==================== Equality ====================

    EQUAL("==", Category.EQUALITY, Arity.BINARY, Source.LITERAL),
    NOT_EQUAL("!=", Category.EQUALITY, Arity.BINARY, Source.LITERAL),

    // ==================== Numeric (literal) ====================

    GREATER_THAN(">", Category.NUMERIC, Arity.BINARY, Source.LITERAL),
    GREATER_OR_EQUAL(">=", Category.NUMERIC, Arity.BINARY, Source.LITERAL),
    LESS_THAN("<", Category.NUMERIC, Arity.BINARY, Source.LITERAL),
    LESS_OR_EQUAL("<=", Category.NUMERIC, Arity.BINARY, Source.LITERAL),

    // ==================== String ====================

    NOT_BLANK("is not blank", Category.STRING, Arity.UNARY, Source.LITERAL),
    STARTS_WITH("starts with", Category.STRING, Arity.BINARY, Source.LITERAL),
    CONTAINS("contains", Category.STRING, Arity.BINARY, Source.LITERAL),
    MATCHES("matches", Category.STRING, Arity.BINARY, Source.LITERAL),
    UPPER_CASE("is uppercase", Category.STRING, Arity.UNARY, Source.LITERAL),
    LOWER_CASE("is lowercase", Category.STRING, Arity.UNARY, Source.LITERAL),

    // ==================== Collection ====================

    NOT_EMPTY("is not empty", Category.COLLECTION, Arity.UNARY, Source.LITERAL),
    COLLECTION_UNIQUE("has unique elements", Category.COLLECTION, Arity.UNARY, Source.LITERAL),

    // ==================== Map ====================

    MAP_NOT_EMPTY("is not empty", Category.MAP, Arity.UNARY, Source.LITERAL),
    MAP_CONTAINS_KEY("contains key", Category.MAP, Arity.BINARY, Source.LITERAL),
    MAP_CONTAINS_KEYS("contains keys", Category.MAP, Arity.BINARY, Source.LITERAL),

    // ==================== Cross-field comparison ====================

    FIELD_LESS_THAN("<", Category.NUMERIC, Arity.BINARY, Source.FIELD),
    FIELD_LESS_OR_EQUAL("<=", Category.NUMERIC, Arity.BINARY, Source.FIELD),
    FIELD_GREATER_THAN(">", Category.NUMERIC, Arity.BINARY, Source.FIELD),
    FIELD_GREATER_OR_EQUAL(">=", Category.NUMERIC, Arity.BINARY, Source.FIELD);

    public enum Category {
        EQUALITY,
        NUMERIC,
        STRING,
        COLLECTION,
        MAP
    }

    public enum Arity {
        UNARY,
        BINARY
    }

    public enum Source {
        LITERAL,
        FIELD
    }

    private final String symbol;
    private final Category category;
    private final Arity arity;
    private final Source source;
}
