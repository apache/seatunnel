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
package org.apache.seatunnel.connectors.seatunnel.campaignmonitor;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorResource.Shape;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Saves the active account page and, when present, its current client's child collection.
 *
 * <p>Only page digests, identifiers and row positions are stored. Credentials and response payloads
 * remain outside the checkpoint.
 */
final class CampaignMonitorSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String scope;
    final List<Frame> frames = new ArrayList<>(2);
    int rootIndex;
    int acceptedPages;

    CampaignMonitorSplit(CampaignMonitorConfig config) {
        this(config.scope);
        frames.add(new Frame(config.roots.get(0), null));
    }

    private CampaignMonitorSplit(String scope) {
        this.scope = scope;
    }

    @Override
    public String splitId() {
        return "campaign-monitor-account";
    }

    boolean finished() {
        return frames.isEmpty();
    }

    /**
     * Copies the complete traversal state for an independent checkpoint.
     *
     * <p>Parent and child progress can advance after the copy is taken.
     */
    CampaignMonitorSplit copy() {
        CampaignMonitorSplit saved = new CampaignMonitorSplit(scope);
        saved.rootIndex = rootIndex;
        saved.acceptedPages = acceptedPages;
        frames.forEach(frame -> saved.frames.add(frame.copy()));
        return saved;
    }

    void validate(CampaignMonitorConfig config) {
        if (!config.scope.equals(scope)) {
            throw failure("credential, collection or date scope changed");
        }
        if (rootIndex < 0
                || rootIndex > config.roots.size()
                || frames.size() > 2
                || finished() != (rootIndex == config.roots.size())
                || acceptedPages < 0
                || acceptedPages > config.scanPages) {
            throw failure("invalid account traversal position");
        }
        if (finished()) {
            return;
        }
        Frame root = frames.get(0);
        root.validate(config);
        if (root.resource != config.roots.get(rootIndex) || root.clientId != null) {
            throw failure("invalid account resource");
        }
        if (frames.size() == 2) {
            Frame child = frames.get(1);
            child.validate(config);
            if (root.resource != CampaignMonitorResource.CLIENTS
                    || !root.emitted
                    || root.child == 0
                    || child.resource != config.children.get(root.child - 1)) {
                throw failure("invalid client collection order");
            }
        }
    }

    void nextRoot(CampaignMonitorConfig config) {
        frames.clear();
        ++rootIndex;
        if (rootIndex < config.roots.size()) {
            frames.add(new Frame(config.roots.get(rootIndex), null));
        }
    }

    private static CampaignMonitorResource.Failure failure(String message) {
        return new CampaignMonitorResource.Failure("checkpoint", message);
    }

    /**
     * Tracks the current row and child boundary in one account or client response.
     *
     * <p>The reader validates the owning client using the restored parent page before child I/O.
     */
    static final class Frame implements Serializable {
        private static final long serialVersionUID = 1L;
        final CampaignMonitorResource resource;
        final String clientId;
        int page = 1;
        int row;
        int child;
        boolean emitted;
        String digest;

        Frame(CampaignMonitorResource resource, String clientId) {
            this.resource = resource;
            this.clientId = clientId;
        }

        Frame copy() {
            Frame saved = new Frame(resource, clientId);
            saved.page = page;
            saved.row = row;
            saved.child = child;
            saved.emitted = emitted;
            saved.digest = digest;
            return saved;
        }

        void validate(CampaignMonitorConfig config) {
            int children = resource == CampaignMonitorResource.CLIENTS ? config.children.size() : 0;
            int rows = config.maxItems;
            if (resource.shape == Shape.PAGED) {
                rows = config.pageSize;
            } else if (resource.shape == Shape.OBJECT) {
                rows = 1;
            }
            if (resource.scoped != (clientId != null)
                    || clientId != null && !CampaignMonitorResource.clientKey(clientId)
                    || page < 1
                    || page > config.maxPages
                    || resource.shape != Shape.PAGED && page != 1
                    || row < 0
                    || row > rows
                    || child < 0
                    || child > children
                    || !emitted && child != 0
                    || digest == null && (row != 0 || emitted)
                    || digest != null && digest.length() != 64) {
                throw failure("invalid response row or page position");
            }
        }

        void advanceRow() {
            ++row;
            child = 0;
            emitted = false;
        }

        void nextPage(int next) {
            page = next;
            row = 0;
            child = 0;
            emitted = false;
            digest = null;
        }
    }
}
