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

public enum ConditionOperator {
    EQUAL("==", Category.EQUALITY, Arity.BINARY, Source.LITERAL, "=="),
    NOT_EQUAL("!=", Category.EQUALITY, Arity.BINARY, Source.LITERAL, "!="),

    GREATER_THAN(">", Category.NUMERIC, Arity.BINARY, Source.LITERAL, ">"),
    GREATER_OR_EQUAL(">=", Category.NUMERIC, Arity.BINARY, Source.LITERAL, ">="),
    LESS_THAN("<", Category.NUMERIC, Arity.BINARY, Source.LITERAL, "<"),
    LESS_OR_EQUAL("<=", Category.NUMERIC, Arity.BINARY, Source.LITERAL, "<="),

    NOT_BLANK("is not blank", Category.STRING, Arity.UNARY, Source.LITERAL, null),
    STARTS_WITH("starts with", Category.STRING, Arity.BINARY, Source.LITERAL, "starts with"),
    STARTS_WITH_IGNORE_CASE(
            "starts with (ignore case)",
            Category.STRING,
            Arity.BINARY,
            Source.LITERAL,
            "starts with (ignore case)"),
    CONTAINS("contains", Category.STRING, Arity.BINARY, Source.LITERAL, "contains"),
    MATCHES("matches", Category.STRING, Arity.BINARY, Source.LITERAL, "matches"),
    UPPER_CASE("is uppercase", Category.STRING, Arity.UNARY, Source.LITERAL, null),
    LOWER_CASE("is lowercase", Category.STRING, Arity.UNARY, Source.LITERAL, null),
    ENDS_WITH("ends with", Category.STRING, Arity.BINARY, Source.LITERAL, "ends with"),
    ENDS_WITH_IGNORE_CASE(
            "ends with (ignore case)",
            Category.STRING,
            Arity.BINARY,
            Source.LITERAL,
            "ends with (ignore case)"),

    LENGTH_EQUAL("length ==", Category.STRING_LENGTH, Arity.BINARY, Source.LITERAL, "length =="),
    LENGTH_GREATER_OR_EQUAL(
            "length >=", Category.STRING_LENGTH, Arity.BINARY, Source.LITERAL, "length >="),
    LENGTH_LESS_OR_EQUAL(
            "length <=", Category.STRING_LENGTH, Arity.BINARY, Source.LITERAL, "length <="),

    NOT_EMPTY("is not empty", Category.COLLECTION, Arity.UNARY, Source.LITERAL, null),
    COLLECTION_UNIQUE(
            "has unique elements", Category.COLLECTION, Arity.UNARY, Source.LITERAL, null),
    COLLECTION_SIZE_EQUAL(
            "size ==", Category.COLLECTION_SIZE, Arity.BINARY, Source.LITERAL, "size =="),
    COLLECTION_SIZE_GREATER_OR_EQUAL(
            "size >=", Category.COLLECTION_SIZE, Arity.BINARY, Source.LITERAL, "size >="),
    COLLECTION_SIZE_LESS_OR_EQUAL(
            "size <=", Category.COLLECTION_SIZE, Arity.BINARY, Source.LITERAL, "size <="),

    FIELD_LESS_THAN("< [field]", Category.NUMERIC, Arity.BINARY, Source.FIELD, "<"),
    FIELD_LESS_OR_EQUAL("<= [field]", Category.NUMERIC, Arity.BINARY, Source.FIELD, "<="),
    FIELD_GREATER_THAN("> [field]", Category.NUMERIC, Arity.BINARY, Source.FIELD, ">"),
    FIELD_GREATER_OR_EQUAL(">= [field]", Category.NUMERIC, Arity.BINARY, Source.FIELD, ">="),
    FIELD_EQUAL("== [field]", Category.EQUALITY, Arity.BINARY, Source.FIELD, "=="),
    FIELD_NOT_EQUAL("!= [field]", Category.EQUALITY, Arity.BINARY, Source.FIELD, "!="),
    FIELD_SIZE_EQUAL(
            "size == [field]", Category.COLLECTION_SIZE, Arity.BINARY, Source.FIELD, "size ==");

    public enum Category {
        EQUALITY,
        NUMERIC,
        STRING,
        STRING_LENGTH,
        COLLECTION,
        COLLECTION_SIZE
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
    private final String displaySymbol;

    ConditionOperator(
            String symbol, Category category, Arity arity, Source source, String displaySymbol) {
        this.symbol = symbol;
        this.category = category;
        this.arity = arity;
        this.source = source;
        this.displaySymbol = displaySymbol;
    }

    public String getSymbol() {
        return symbol;
    }

    public Category getCategory() {
        return category;
    }

    public Arity getArity() {
        return arity;
    }

    public Source getSource() {
        return source;
    }

    public String getDisplaySymbol() {
        return displaySymbol;
    }
}
