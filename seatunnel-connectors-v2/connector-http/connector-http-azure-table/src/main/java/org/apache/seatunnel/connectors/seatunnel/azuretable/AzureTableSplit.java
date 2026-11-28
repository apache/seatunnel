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
package org.apache.seatunnel.connectors.seatunnel.azuretable;

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Resumable position in Azure account discovery or one selected table's entity scan.
 *
 * <p>The state carries table names, opaque continuation and the row offset. Entity payloads,
 * cryptographic key material and signed request headers stay outside the split.
 */
final class AzureTableSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private final String scope;

    // Discovery completes before reading entities so every selected table is represented once.
    final List<String> tables = new ArrayList<>();
    private final Set<String> knownTables = new HashSet<>();
    boolean discovering;
    boolean finished;
    int tableIndex;

    // The next-token history belongs to one discovery listing or one table, never both.
    AzureTableApi.Cursor cursor;
    private final Set<String> seen = new HashSet<>();
    int page;
    int row;
    String digest;
    Set<String> previous = Set.of();

    AzureTableSplit(String scope, List<String> selected) {
        this.scope = scope;
        tables.addAll(selected);
        selected.forEach(table -> knownTables.add(AzureTableConfig.normalized(table)));
        discovering = selected.isEmpty();
    }

    @Override
    public String splitId() {
        return "azure-table-account-scan";
    }

    boolean finished() {
        return finished;
    }

    String table() {
        return tables.get(tableIndex);
    }

    /**
     * Copies the coordinator or reader position without sharing mutable catalog and token state.
     *
     * <p>The previous-page hashes and token tuple are immutable.
     */
    AzureTableSplit copy() {
        AzureTableSplit saved = new AzureTableSplit(scope, tables);
        saved.discovering = discovering;
        saved.finished = finished;
        saved.tableIndex = tableIndex;
        saved.cursor = cursor;
        saved.seen.addAll(seen);
        saved.page = page;
        saved.row = row;
        saved.digest = digest;
        saved.previous = previous;
        return saved;
    }

    void validate(AzureTableConfig config) {
        if (!config.scope.equals(scope)
                || tables.size() > config.maxTables
                || tableIndex < 0
                || tableIndex > tables.size()
                || !config.tables.isEmpty() && !config.tables.equals(tables)
                || !discovering && !finished && tableIndex == tables.size()
                || discovering
                        && (finished
                                || tableIndex != 0
                                || row != 0
                                || digest != null
                                || cursor != null && cursor.second != null)
                || page < 0
                || row < 0
                || row > config.pageSize
                || row > 0 && digest == null
                || page > 0 && cursor == null) {
            throw new IllegalStateException(
                    "AzureTableStorage checkpoint has incompatible scan options or state");
        }
    }

    /**
     * Adds every table in a verified listing page before moving its saved continuation.
     *
     * <p>An empty page can still have a continuation. The scan is empty only after the final
     * listing page and only if Azure returned no table names.
     */
    void commitDiscovery(AzureTableApi.DiscoveryPage value, AzureTableConfig config) {
        String collection = AzureTableApi.DISCOVERY;
        if (value.names.size() > config.maxTables - tables.size()) {
            throw new AzureTableApi.Failure(collection, "exceeded max_tables");
        }
        Set<String> current = new HashSet<>();
        for (String table : value.names) {
            String name = AzureTableConfig.normalized(table);
            if (knownTables.contains(name) || !current.add(name)) {
                throw new AzureTableApi.Failure(collection, "repeated table name in discovery");
            }
        }
        if (value.next != null) {
            next(collection, value.next, config.maxPages);
            tables.addAll(value.names);
            knownTables.addAll(current);
            return;
        }
        List<String> selected = new ArrayList<>(tables);
        selected.addAll(value.names);
        config.validateBoundaries(selected);
        tables.addAll(value.names);
        knownTables.addAll(current);
        discovering = false;
        finished = tables.isEmpty();
        resetPage();
    }

    /**
     * Advances the Azure cursor only after the complete current page has been consumed.
     *
     * <p>Table completion resets continuation guards so independent tables may return identical
     * opaque continuation strings.
     */
    void commitEntityPage(AzureTableApi.EntityPage value, AzureTableConfig config) {
        if (value.next != null) {
            next(table(), value.next, config.maxPages);
            if (!value.rows.isEmpty()) {
                previous =
                        value.rows.stream()
                                .map(entity -> entity.identity)
                                .collect(Collectors.toUnmodifiableSet());
            }
        } else {
            ++tableIndex;
            finished = tableIndex == tables.size();
            resetPage();
        }
    }

    private void next(String collection, AzureTableApi.Cursor next, int maximum) {
        if (page + 1 >= maximum) {
            throw new AzureTableApi.Failure(collection, "exceeded max_pages_per_collection");
        }
        if (!seen.add(next.fingerprint())) {
            throw new AzureTableApi.Failure(collection, "repeated continuation token");
        }
        cursor = next;
        ++page;
        row = 0;
        digest = null;
    }

    private void resetPage() {
        page = 0;
        row = 0;
        digest = null;
        cursor = null;
        seen.clear();
        previous = Set.of();
    }
}
