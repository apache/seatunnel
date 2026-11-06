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

package org.apache.seatunnel.lineage;

import java.util.Optional;
import java.util.regex.Pattern;

/** Canonical namespace/name mappings for supported table connectors. */
public final class LineageDatasetNaming {
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{[^}]*}");

    private static final String DEFAULT_TABLE_PATH = "default.default.default";

    private LineageDatasetNaming() {}

    /**
     * Creates the Paimon dataset identity {@code paimon://catalog/database} and {@code table}.
     *
     * <p>The catalog is required because it is part of the dataset identity.
     */
    public static Optional<LineageDataset> paimon(
            String catalogName, String database, String table) {
        if (isUnusable(catalogName) || isUnusable(database) || isUnusable(table)) {
            return Optional.empty();
        }
        return Optional.of(LineageDataset.of("paimon://" + catalogName + "/" + database, table));
    }

    /** Creates the Doris dataset identity using its query port rather than its HTTP port. */
    public static Optional<LineageDataset> doris(
            String feHost, int queryPort, String database, String table) {
        return jdbc("mysql", feHost, queryPort, database, table);
    }

    /** Creates a JDBC dataset identity from the URL scheme, host, port, database, and table. */
    public static Optional<LineageDataset> jdbc(
            String scheme, String host, int port, String database, String table) {
        if (isUnusable(scheme)
                || isUnusable(host)
                || port <= 0
                || isUnusable(database)
                || isUnusable(table)) {
            return Optional.empty();
        }
        return Optional.of(
                LineageDataset.of(scheme + "://" + host + ":" + port, database + "." + table));
    }

    /** Returns whether a complete table path is the placeholder default path. */
    public static boolean isDefaultTablePath(String fullName) {
        return fullName != null && DEFAULT_TABLE_PATH.equals(fullName.trim());
    }

    /**
     * Returns whether a name component cannot identify a real object.
     *
     * <p>Besides an absent value this covers an unresolved placeholder: a multi-table sink routes
     * rows with a template such as {@code table = "${table_name}"}, substituted per row at write
     * time, so it is never a real table. Emitting it verbatim would collapse every
     * placeholder-routed job onto one shared node in the lineage graph — the same failure mode as
     * the {@code default.default.default} path, but harder to spot because the node carries a
     * plausible-looking name.
     */
    private static boolean isUnusable(String value) {
        if (value == null || value.trim().isEmpty()) {
            return true;
        }
        return PLACEHOLDER.matcher(value).find();
    }
}
