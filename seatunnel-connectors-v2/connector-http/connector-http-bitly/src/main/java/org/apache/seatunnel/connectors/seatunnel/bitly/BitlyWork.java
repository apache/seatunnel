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
package org.apache.seatunnel.connectors.seatunnel.bitly;

import okhttp3.HttpUrl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * One account-scoped request and its current provider continuation.
 *
 * <p>The path is built from fixed route segments and previously validated provider identifiers.
 * External links carried by response records are never used as destinations.
 */
final class BitlyWork implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Internal request stages associated with the public output resources.
     *
     * <p>The nonemitting QR list stage discovers coupled IDs. A detail stage may retrieve its image
     * before producing a single output row.
     */
    enum Stage {
        BSDS(BitlyResource.BSDS),
        CAMPAIGNS(BitlyResource.CAMPAIGNS),
        CHANNELS(BitlyResource.CHANNELS),
        ORGANIZATIONS(BitlyResource.ORGANIZATIONS),
        ORGANIZATION_COUNTS(BitlyResource.ORGANIZATION_COUNTS),
        GROUPS(BitlyResource.GROUPS),
        GROUP_PREFERENCES(BitlyResource.GROUP_PREFERENCES),
        GROUP_COUNTS(BitlyResource.GROUP_COUNTS),
        BITLINKS(BitlyResource.BITLINKS),
        CLICKS(BitlyResource.CLICKS),
        QR_LIST(BitlyResource.QR_CODES),
        QR_DETAIL(BitlyResource.QR_CODES),
        QR_LEGACY(BitlyResource.QR_CODES);

        final BitlyResource resource;

        Stage(BitlyResource resource) {
            this.resource = resource;
        }
    }

    final Stage stage;
    final String org;
    final String group;
    final String link;
    final String qr;
    final String cursor;
    final int page;
    // Histories are limited by max_pages_per_parent and dropped after the terminal page.
    final Set<String> history;
    final Set<String> previousIds;

    BitlyWork(Stage stage, String org, String group, String link, String qr) {
        this(stage, org, group, link, qr, null, 0, Set.of(), Set.of());
    }

    private BitlyWork(
            Stage stage,
            String org,
            String group,
            String link,
            String qr,
            String cursor,
            int page,
            Set<String> history,
            Set<String> previousIds) {
        this.stage = stage;
        this.org = org;
        this.group = group;
        this.link = link;
        this.qr = qr;
        this.cursor = cursor;
        this.page = page;
        this.history = history;
        this.previousIds = previousIds;
    }

    /**
     * Builds the initial plan once for all selected resources.
     *
     * <p>Organizations and groups are read exactly once per scope even when their own output is
     * disabled or falls outside the date window.
     */
    static List<BitlyWork> initial(BitlyConfig config) {
        List<BitlyWork> work = new ArrayList<>();
        for (Stage stage : List.of(Stage.BSDS, Stage.CAMPAIGNS, Stage.CHANNELS)) {
            if (config.includes(stage.resource)) {
                work.add(new BitlyWork(stage, null, null, null, null));
            }
        }
        if (config.includes(BitlyResource.ORGANIZATIONS)
                || config.includes(BitlyResource.ORGANIZATION_COUNTS)
                || config.groupsNeeded()) {
            work.add(new BitlyWork(Stage.ORGANIZATIONS, null, null, null, null));
        }
        return work;
    }

    BitlyReadException failure(String reason) {
        return new BitlyReadException(stage.resource, reason);
    }

    BitlyWork next(String after, String identity, Set<String> ids) throws BitlyReadException {
        String marker = "cursor:" + BitlyConfig.digest(after);
        if (history.contains(marker)) {
            throw failure("provider repeated a continuation cursor");
        }
        Set<String> completed = new HashSet<>(history);
        completed.add(marker);
        if (identity != null) {
            completed.add(identity);
        }
        return new BitlyWork(
                stage,
                org,
                group,
                link,
                qr,
                after,
                page + 1,
                Set.copyOf(completed),
                Set.copyOf(ids));
    }

    /**
     * Builds the request path using the v4 endpoint contract.
     *
     * <p>Bitlinks use two validated path segments, as in the provider's documented requests. The
     * cursor is encoded as one opaque query value.
     */
    HttpUrl url(HttpUrl origin, BitlyConfig config) {
        HttpUrl.Builder target = origin.newBuilder().addPathSegment("v4");
        switch (stage) {
            case BSDS:
            case CAMPAIGNS:
            case CHANNELS:
            case ORGANIZATIONS:
                return target.addPathSegment(stage.resource.name).build();
            case ORGANIZATION_COUNTS:
                return metrics(
                        target.addPathSegment("organizations")
                                .addPathSegment(org)
                                .addPathSegment("shorten_counts"),
                        config);
            case GROUPS:
                return target.addPathSegment("groups")
                        .addQueryParameter("organization_guid", org)
                        .build();
            case GROUP_PREFERENCES:
                return groupPath(target, "preferences").build();
            case GROUP_COUNTS:
                return metrics(groupPath(target, "shorten_counts"), config);
            case BITLINKS:
                target = paged(groupPath(target, "bitlinks"), config);
                if (!config.linksNeededForChildren()) {
                    target.addQueryParameter(
                                    "created_after", Long.toString(config.queryStartSecond()))
                            .addQueryParameter(
                                    "created_before", Long.toString(config.queryEndSecond()));
                }
                return target.build();
            case CLICKS:
                return metrics(linkPath(target, "clicks"), config);
            case QR_LIST:
                return paged(groupPath(target, "qr-codes"), config)
                        .addQueryParameter("qrc_type", "bitlink")
                        .build();
            case QR_DETAIL:
                return target.addPathSegment("qr-codes").addPathSegment(qr).build();
            case QR_LEGACY:
                return linkPath(target, "qr").build();
            default:
                throw new IllegalStateException("Unknown Bitly request stage");
        }
    }

    private HttpUrl.Builder groupPath(HttpUrl.Builder target, String child) {
        return target.addPathSegment("groups").addPathSegment(group).addPathSegment(child);
    }

    private HttpUrl.Builder linkPath(HttpUrl.Builder target, String child) {
        int split = link.indexOf('/');
        return target.addPathSegment("bitlinks")
                .addPathSegment(link.substring(0, split))
                .addPathSegment(link.substring(split + 1))
                .addPathSegment(child);
    }

    private HttpUrl.Builder paged(HttpUrl.Builder target, BitlyConfig config) {
        target.addQueryParameter("size", Integer.toString(config.pageSize))
                .addQueryParameter("archived", "both");
        if (cursor != null) {
            target.addQueryParameter("search_after", cursor);
        }
        return target;
    }

    private static HttpUrl metrics(HttpUrl.Builder target, BitlyConfig config) {
        return target.addQueryParameter("unit", "day")
                .addQueryParameter("units", "-1")
                .addQueryParameter("unit_reference", config.metricReference)
                .build();
    }
}
