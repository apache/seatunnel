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
package org.apache.seatunnel.connectors.seatunnel.buildkite;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteResource.Shape;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Holds the frozen window and active collection stack for one bounded scan.
 *
 * <p>Each visited parent stays on the stack until all of the current row's selected children
 * finish. Page content and authentication material are absent from the checkpoint.
 */
final class BuildkiteSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String scope;
    final Instant end;
    final List<Frame> frames = new ArrayList<>(3);
    int rootIndex;
    int acceptedPages;

    BuildkiteSplit(BuildkiteConfig config, Instant end) {
        this(config.scope, end);
        frames.add(new Frame(config.roots.get(0), null, null));
    }

    private BuildkiteSplit(String scope, Instant end) {
        this.scope = scope;
        this.end = end;
    }

    @Override
    public String splitId() {
        return "buildkite-bounded-scan";
    }

    boolean finished() {
        return frames.isEmpty();
    }

    /**
     * Detaches the child and parent cursor positions for a coordinator or reader snapshot.
     *
     * <p>A parent row remains pinned until the last selected child has completed.
     */
    BuildkiteSplit copy() {
        BuildkiteSplit saved = new BuildkiteSplit(scope, end);
        saved.rootIndex = rootIndex;
        saved.acceptedPages = acceptedPages;
        frames.forEach(frame -> saved.frames.add(frame.copy()));
        return saved;
    }

    /**
     * Validates scope and stack consistency before a recovered reader may make a request.
     *
     * <p>The active frames must follow the selected root and immediate parent order.
     */
    void validate(BuildkiteConfig config) {
        if (!config.scope.equals(scope)
                || end == null
                || config.end != null && !config.end.equals(end)) {
            throw checkpointFailure("credential, selection or creation window changed");
        }
        if (rootIndex < 0
                || rootIndex > config.roots.size()
                || acceptedPages < 0
                || acceptedPages > config.scanPages
                || frames.size() > 3
                || finished() != (rootIndex == config.roots.size())) {
            throw checkpointFailure("invalid root or page budget");
        }
        if (finished()) {
            return;
        }
        Frame top = frames.get(0);
        if (top.resource != config.roots.get(rootIndex)
                || top.org != null
                || top.partition != null) {
            throw checkpointFailure("invalid root collection");
        }
        for (int depth = 0; depth < frames.size(); ++depth) {
            Frame frame = frames.get(depth);
            frame.validate(config);
            if (depth == 0) {
                continue;
            }
            Frame parent = frames.get(depth - 1);
            List<BuildkiteResource> children = config.children(parent.resource);
            if (!parent.emitted
                    || parent.child == 0
                    || frame.resource != children.get(parent.child - 1)
                    || parent.resource != BuildkiteResource.ORGANIZATIONS
                            && !Objects.equals(parent.org, frame.org)) {
                throw checkpointFailure("invalid parent and child collection order");
            }
        }
    }

    void nextRoot(BuildkiteConfig config) {
        frames.clear();
        ++rootIndex;
        if (rootIndex < config.roots.size()) {
            frames.add(new Frame(config.roots.get(rootIndex), null, null));
        }
    }

    private static BuildkiteResource.Failure checkpointFailure(String reason) {
        return new BuildkiteResource.Failure("checkpoint", reason);
    }

    /**
     * Records the exact page, row and child position of one collection.
     *
     * <p>Only an organization slug and, when needed, a pipeline slug or cluster ID are stored. The
     * reader revalidates both the parent page digest and its child key on restore.
     */
    static final class Frame implements Serializable {
        private static final long serialVersionUID = 1L;

        final BuildkiteResource resource;
        final String org;
        final String partition;
        int page = 1;
        int row;
        boolean emitted;
        int child;
        String digest;

        Frame(BuildkiteResource resource, String org, String partition) {
            this.resource = resource;
            this.org = org;
            this.partition = partition;
        }

        Frame copy() {
            Frame saved = new Frame(resource, org, partition);
            saved.page = page;
            saved.row = row;
            saved.emitted = emitted;
            saved.child = child;
            saved.digest = digest;
            return saved;
        }

        void validate(BuildkiteConfig config) {
            boolean singlePage = resource.shape == Shape.ARRAY || resource.shape == Shape.SINGLE;
            boolean childOfOrg = resource.parent == BuildkiteResource.ORGANIZATIONS;
            boolean needsPartition = resource.parent != null && !childOfOrg;
            int limit = config.unpagedItems;
            if (resource.shape == Shape.PAGED) {
                limit = config.pageSize;
            } else if (resource.shape == Shape.SINGLE) {
                limit = 1;
            }
            if (resource.parent != null && !BuildkiteResource.pathKey(org)
                    || needsPartition && !BuildkiteResource.pathKey(partition)
                    || !needsPartition && partition != null
                    || page < 1
                    || page > config.maxPages
                    || singlePage && page != 1
                    || row < 0
                    || row > limit
                    || child < 0
                    || child > config.children(resource).size()
                    || !emitted && child != 0
                    || digest == null && (row != 0 || emitted)
                    || digest != null && digest.length() != 64) {
                throw checkpointFailure("invalid collection page or row");
            }
        }

        void advanceRow() {
            ++row;
            emitted = false;
            child = 0;
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
