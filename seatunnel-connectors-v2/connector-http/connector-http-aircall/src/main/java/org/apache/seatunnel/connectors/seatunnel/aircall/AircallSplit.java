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
package org.apache.seatunnel.connectors.seatunnel.aircall;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;

/** Position of a parent collection and its current child, with no stored row bodies. */
final class AircallSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private static final int MAX_WINDOWS = 100000;
    final int version = 1;
    final byte[] identity;
    final long end;
    int group = -1;
    long from;
    long until;
    int windows;
    Cursor root = new Cursor();
    boolean parentCommitted;
    int childIndex;
    Cursor child = new Cursor();

    AircallSplit(byte[] identity, long end) {
        this.identity = identity.clone();
        this.end = end;
    }

    AircallSplit copy() {
        AircallSplit copy = new AircallSplit(identity, end);
        copy.group = group;
        copy.from = from;
        copy.until = until;
        copy.windows = windows;
        copy.root = root.copy();
        copy.parentCommitted = parentCommitted;
        copy.childIndex = childIndex;
        copy.child = child.copy();
        return copy;
    }

    void nextGroup(AircallConfig config) {
        group++;
        from = 0;
        until = 0;
        windows = 0;
        root = new Cursor();
        resetParent();
        if (group < config.groups.size() && config.groups.get(group).root.windowed()) {
            AircallResource resource = config.groups.get(group).root;
            openWindow(config, resource, config.first(resource, end));
        }
    }

    private void openWindow(AircallConfig config, AircallResource resource, long lower) {
        from = lower;
        until = Math.min(end, lower + config.window(resource));
        chargeWindow(resource);
    }

    /** The current window is still uncommitted. Narrow it before yielding a row. */
    void narrow(AircallConfig config, AircallResource resource) {
        if (root.page != 1 || root.index != 0 || parentCommitted || !resource.splittable()) {
            throw resource.failure(
                    "10,000-result boundary cannot be recovered without dropping rows");
        }
        if (from == 0) {
            openWindow(config, resource, AircallConfig.EARLIEST);
        } else {
            long length = until - from;
            if (length <= 1) {
                throw resource.failure(
                        "10,000-result boundary in one second; cannot subdivide further");
            }
            until = from + length / 2;
            chargeWindow(resource);
        }
    }

    private void chargeWindow(AircallResource resource) {
        if (++windows > MAX_WINDOWS) {
            throw resource.failure("window limit exceeded");
        }
    }

    void nextRootPage(AircallConfig config, AircallResource resource, AircallPage page) {
        resetParent();
        if (page.more) {
            root.advance(page);
        } else if (until != 0 && until < end) {
            openWindow(config, resource, until);
            root = new Cursor();
        } else {
            nextGroup(config);
        }
    }

    void resetParent() {
        parentCommitted = false;
        childIndex = 0;
        child = new Cursor();
    }

    void validate(AircallConfig config) {
        boolean terminal = group == config.groups.size();
        int children = terminal ? 0 : config.groups.get(group).children.size();
        if (windows < 0
                || windows > MAX_WINDOWS
                || ((from == 0) != (until == 0))
                || (from != 0
                        && (from < AircallConfig.EARLIEST
                                || until <= from
                                || until > end
                                || terminal
                                || !config.groups.get(group).root.splittable()))
                || childIndex < 0
                || childIndex > children
                || (parentCommitted && children == 0)
                || (!parentCommitted && (childIndex != 0 || child.page != 1 || child.index != 0))
                || (terminal && (root.page != 1 || root.index != 0))) {
            throw new IllegalStateException(
                    "Aircall checkpoint has an invalid acquisition boundary");
        }
        root.validate(config, parentCommitted);
        child.validate(config, false);
    }

    @Override
    public String splitId() {
        return "aircall-account-scan";
    }

    static final class Cursor implements Serializable {
        private static final long serialVersionUID = 1L;
        private static final int HISTORY_LIMIT = 256;
        int page = 1;
        String token;
        int index;
        byte[] digest = new byte[0];
        final Deque<String> recentlySeen = new ArrayDeque<>();

        Cursor copy() {
            Cursor copy = new Cursor();
            copy.page = page;
            copy.token = token;
            copy.index = index;
            copy.digest = digest.clone();
            copy.recentlySeen.addAll(recentlySeen);
            return copy;
        }

        void advance(AircallPage pageResult) {
            page++;
            token = pageResult.continuation;
            index = 0;
            digest = new byte[0];
            remember(pageResult.fingerprint);
            if (token != null) {
                remember(AircallPage.cursorFingerprint(token));
            }
        }

        private void remember(String value) {
            if (value != null) {
                if (recentlySeen.size() == HISTORY_LIMIT) {
                    recentlySeen.removeFirst();
                }
                recentlySeen.addLast(value);
            }
        }

        private void validate(AircallConfig config, boolean committedParent) {
            if (page < 1
                    || page > config.maxPages
                    || (token != null && token.length() > AircallResource.MAX_CURSOR)
                    || index < 0
                    || index >= AircallResource.QUERY_CAP
                    || recentlySeen.size() > HISTORY_LIMIT
                    || (index != 0 || committedParent ? digest.length != 32 : digest.length != 0)) {
                throw new IllegalStateException("Aircall checkpoint has an invalid page position");
            }
        }
    }
}
