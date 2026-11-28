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

package org.apache.seatunnel.connectors.seatunnel.activecampaign;

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.ArrayDeque;
import java.util.Deque;

/** Checkpoints hold positions and verification digests, without raw records or credentials. */
final class ActiveCampaignSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    private static final byte[] NO_PAGE = new byte[0];
    static final int HISTORY_LIMIT = 128;
    static final int EMPTY_LIMIT = 10;
    final int version = 1;
    final byte[] identity;
    final Deque<String> history = new ArrayDeque<>();
    int resource;
    int offset;
    int emitted;
    int emptyPages;
    byte[] pageDigest = NO_PAGE;

    ActiveCampaignSplit(byte[] identity) {
        this.identity = identity.clone();
    }

    ActiveCampaignSplit copy() {
        ActiveCampaignSplit copy = new ActiveCampaignSplit(identity);
        copy.resource = resource;
        copy.offset = offset;
        copy.emitted = emitted;
        copy.emptyPages = emptyPages;
        copy.pageDigest = pageDigest.clone();
        copy.history.addAll(history);
        return copy;
    }

    /** Advance the page together with its committed output state. */
    void completePage(int nextOffset, String fingerprint, boolean empty) {
        emitted = 0;
        pageDigest = NO_PAGE;
        if (nextOffset == -1) {
            resource++;
            offset = 0;
            emptyPages = 0;
            history.clear();
        } else {
            offset = nextOffset;
            emptyPages = empty ? emptyPages + 1 : 0;
            if (fingerprint != null) {
                if (history.size() == HISTORY_LIMIT) {
                    history.removeFirst();
                }
                history.addLast(fingerprint);
            }
        }
    }

    @Override
    public String splitId() {
        return "activecampaign-scan";
    }
}
