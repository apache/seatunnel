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
package org.apache.seatunnel.connectors.seatunnel.akeneo;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

final class AkeneoSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    static final String ID = "akeneo-catalog";

    final String scope;
    int group;
    Position root = new Position();
    int parent;
    boolean familyEmitted;
    Position child = new Position();
    String familyPageDigest;

    AkeneoSplit(String scope) {
        this.scope = scope;
    }

    @Override
    public String splitId() {
        return ID;
    }

    AkeneoSplit copy() {
        AkeneoSplit result = new AkeneoSplit(scope);
        result.group = group;
        result.root = root.copy();
        result.parent = parent;
        result.familyEmitted = familyEmitted;
        result.child = child.copy();
        result.familyPageDigest = familyPageDigest;
        return result;
    }

    void nextGroup() {
        group++;
        root = new Position();
        clearFamilyPage();
    }

    void clearFamilyPage() {
        parent = 0;
        familyEmitted = false;
        child = new Position();
        familyPageDigest = null;
    }

    void validate(AkeneoConfig config) {
        if (!config.scope.equals(scope)) {
            throw new IllegalArgumentException(
                    "Akeneo checkpoint does not match the configured source scope");
        }
        if (group < 0
                || group > config.groups.size()
                || root == null
                || child == null
                || parent < 0
                || parent > config.pageSize) {
            throw new IllegalArgumentException("Akeneo invalid checkpoint position");
        }
        boolean active = group < config.groups.size();
        AkeneoResource resource = active ? config.groups.get(group) : null;
        int rootLimit = active && resource.unpaged(config) ? config.maxBytes : config.pageSize;
        root.validate(resource == AkeneoResource.PRODUCTS, rootLimit, config.maxPages);
        child.validate(false, config.pageSize, config.maxPages);
        boolean variants =
                resource == AkeneoResource.FAMILIES
                        && config.selection.contains(AkeneoResource.FAMILY_VARIANTS);
        if (!active && !root.initial()
                || !variants
                        && (familyPageDigest != null
                                || parent != 0
                                || familyEmitted
                                || !child.initial())
                || variants
                        && ((familyPageDigest == null
                                        && (parent != 0 || familyEmitted || !child.initial()))
                                || (familyPageDigest != null
                                        && !AkeneoPage.isHash(familyPageDigest))
                                || !familyEmitted && !child.initial()
                                || root.offset != 0)) {
            throw new IllegalArgumentException("Akeneo invalid checkpoint traversal");
        }
    }

    static final class Position implements Serializable {
        private static final long serialVersionUID = 1L;
        int number = 1;
        String cursor;
        private Set<String> priorCursors;
        int offset;
        String prefix;

        Position copy() {
            Position result = new Position();
            result.number = number;
            result.cursor = cursor;
            result.priorCursors = priorCursors == null ? null : new HashSet<>(priorCursors);
            result.offset = offset;
            result.prefix = prefix;
            return result;
        }

        boolean cursorSeen(String next) {
            return priorCursors != null && priorCursors.contains(AkeneoPage.hash(next));
        }

        void advance(AkeneoResource resource, String cursor, int maxPages) {
            if (number >= maxPages) {
                throw resource.failure("max_pages reached with a continuation");
            }
            if (cursor != null) {
                if (priorCursors == null) {
                    priorCursors = new HashSet<>();
                }
                if (!priorCursors.add(AkeneoPage.hash(cursor))) {
                    throw resource.failure("repeated search-after cursor");
                }
            }
            this.cursor = cursor;
            number++;
            offset = 0;
            prefix = null;
        }

        boolean initial() {
            return number == 1 && offset == 0;
        }

        private void validate(boolean products, int limit, int maxPages) {
            if (number < 1
                    || number > maxPages
                    || offset < 0
                    || offset > limit
                    || (offset == 0 ? prefix != null : !AkeneoPage.isHash(prefix))) {
                throw new IllegalArgumentException("Akeneo invalid checkpoint page");
            }
            if (!products || number == 1) {
                if (cursor != null || priorCursors != null) {
                    throw new IllegalArgumentException("Akeneo invalid checkpoint cursor");
                }
            } else if (!AkeneoPage.isCursor(cursor)
                    || priorCursors == null
                    || priorCursors.size() != number - 1
                    || !priorCursors.contains(AkeneoPage.hash(cursor))) {
                throw new IllegalArgumentException("Akeneo invalid checkpoint cursor");
            }
        }
    }
}
