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

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

public class CatalogUtils {
    public static String getFieldIde(String identifier, String fieldIde) {
        if (fieldIde == null) {
            return identifier;
        }
        switch (FieldIdeEnum.valueOf(fieldIde.toUpperCase())) {
            case LOWERCASE:
                return identifier.toLowerCase();
            case UPPERCASE:
                return identifier.toUpperCase();
            default:
                return identifier;
        }
    }

    public static String quoteIdentifier(String identifier, String fieldIde, String quote) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append(quote).append(parts[i]).append(quote).append(".");
            }
            return sb.append(quote)
                    .append(getFieldIde(parts[parts.length - 1], fieldIde))
                    .append(quote)
                    .toString();
        }

        return quote + getFieldIde(identifier, fieldIde) + quote;
    }

    public static String quoteIdentifier(String identifier, String fieldIde) {
        return getFieldIde(identifier, fieldIde);
    }

    public static String quoteTableIdentifier(String identifier, String fieldIde) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append(parts[i]).append(".");
            }
            return sb.append(getFieldIde(parts[parts.length - 1], fieldIde)).toString();
        }

        return getFieldIde(identifier, fieldIde);
    }
}
