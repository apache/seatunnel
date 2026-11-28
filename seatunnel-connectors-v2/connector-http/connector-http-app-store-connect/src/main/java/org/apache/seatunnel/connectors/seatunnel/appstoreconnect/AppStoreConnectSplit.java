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
package org.apache.seatunnel.connectors.seatunnel.appstoreconnect;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectResource.Stage;

/**
 * Checkpoint for one bounded resource scan.
 *
 * <p>The bounded relationship stack, closed period and offsets can be resumed on another reader.
 * Only stable resource IDs and opaque API cursors are saved. JWTs, downloadable URLs and temporary
 * files are excluded.
 */
final class AppStoreConnectSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    // Nonsecret logical options determine which rows a saved offset can refer to.
    private final String scope;

    // Frozen once by the enumerator so a restore on another day keeps the same period bounds.
    final Instant cutoff;

    // Index into the caller's selected resource order, with size signifying completion.
    int resource;

    // Relationship pages and parent IDs on the active path. Maximum depth is six.
    final List<Frame> stack = new ArrayList<>();

    // Current date (or first of the current fiscal month) for the account reports.
    LocalDate reportDate;

    // Digest binds a TSV row offset to the original verified contents.
    String reportDigest;

    long reportOffset;

    AppStoreConnectSplit(String scope, Instant cutoff) {
        this.scope = scope;
        this.cutoff = cutoff;
    }

    @Override
    public String splitId() {
        return "app-store-connect-catalog";
    }

    boolean finished(AppStoreConnectConfig config) {
        return resource == config.resources.size();
    }

    /**
     * Makes a checkpoint independent of subsequent in-memory reader mutations.
     *
     * <p>Each frame copies its cycle guard while sharing only immutable lineage and scalar fields.
     */
    AppStoreConnectSplit copy() {
        AppStoreConnectSplit copy = new AppStoreConnectSplit(scope, cutoff);
        copy.resource = resource;
        for (Frame frame : stack) {
            copy.stack.add(frame.copy());
        }
        copy.reportDate = reportDate;
        copy.reportDigest = reportDigest;
        copy.reportOffset = reportOffset;
        return copy;
    }

    void nextResource() {
        resource++;
        stack.clear();
        reportDate = null;
        clearReport();
    }

    void clearReport() {
        reportDigest = null;
        reportOffset = 0;
    }

    /**
     * Detects incompatible restore settings before discovery or token signing.
     *
     * <p>Credential rotation and request time limits do not change the logical scan.
     */
    void validate(AppStoreConnectConfig config) {
        if (!config.scope.equals(scope)
                || cutoff == null
                || resource < 0
                || resource > config.resources.size()
                || stack.size() > 6
                || reportOffset < 0
                || reportDigest == null && reportOffset != 0
                || finished(config)
                        && (!stack.isEmpty() || reportDate != null || reportDigest != null)) {
            throw new IllegalStateException(
                    "AppStoreConnect checkpoint has incompatible scan options or state");
        }
    }

    /**
     * Stores a parent-specific collection page and its next unprocessed record.
     *
     * <p>A parent records its current row as visited when the child frame is added, so the child
     * can finish after restoration without processing the parent twice.
     */
    static final class Frame implements Serializable {
        private static final long serialVersionUID = 1L;

        final Stage stage;
        final AppStoreConnectApi.Parents parents;
        String cursor;
        String digest;
        int offset;
        final Set<String> seen = new HashSet<>();

        Frame(Stage stage, AppStoreConnectApi.Parents parents) {
            this.stage = stage;
            this.parents = parents;
        }

        void advance(AppStoreConnectResource resource, String next, int maximum) {
            if (seen.size() >= maximum - 1 || !seen.add(AppStoreConnectConfig.hash(next))) {
                throw resource.failure(
                        "pagination repeated a cursor or exceeded max_pages_per_parent");
            }
            cursor = next;
            digest = null;
            offset = 0;
        }

        private Frame copy() {
            Frame frame = new Frame(stage, parents);
            frame.cursor = cursor;
            frame.digest = digest;
            frame.offset = offset;
            frame.seen.addAll(seen);
            return frame;
        }
    }
}
