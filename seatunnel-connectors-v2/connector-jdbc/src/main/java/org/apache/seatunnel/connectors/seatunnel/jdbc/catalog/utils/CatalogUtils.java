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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.IdentifierCase;

import org.apache.commons.lang3.StringUtils;

public class CatalogUtils {
    public static String getIdentifierCase(String identifier, String identifierCase) {
        if (StringUtils.isBlank(identifierCase)) {
            return identifier;
        }
        switch (IdentifierCase.valueOf(identifierCase.toUpperCase())) {
            case LOWERCASE:
                return identifier.toLowerCase();
            case UPPERCASE:
                return identifier.toUpperCase();
            default:
                return identifier;
        }
    }

    public static String quoteIdentifier(String identifier, String identifierCase, String quote) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append(quote).append(parts[i]).append(quote).append(".");
            }
            return sb.append(quote)
                    .append(getIdentifierCase(parts[parts.length - 1], identifierCase))
                    .append(quote)
                    .toString();
        }

        return quote + getIdentifierCase(identifier, identifierCase) + quote;
    }

    public static String quoteIdentifier(String identifier, String identifierCase) {
        return getIdentifierCase(identifier, identifierCase);
    }

    public static String quoteTableIdentifier(String identifier, String identifierCase) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append(parts[i]).append(".");
            }
            return sb.append(getIdentifierCase(parts[parts.length - 1], identifierCase)).toString();
        }

        return getIdentifierCase(identifier, identifierCase);
    }
}
