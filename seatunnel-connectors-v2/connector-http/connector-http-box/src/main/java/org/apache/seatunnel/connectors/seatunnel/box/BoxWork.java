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
package org.apache.seatunnel.connectors.seatunnel.box;

import okhttp3.HttpUrl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A resource read scoped to one Box object and one provider position.
 *
 * <p>Routes are assembled from validated identifiers and operation constants. Provider-supplied
 * content locations are handled separately by the download implementation.
 */
final class BoxWork implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Continuation model for each selected endpoint.
     *
     * <p>Offset paging is limited to routes without a documented marker alternative.
     */
    enum Paging {
        NONE,
        OFFSET,
        MARKER,
        EVENT_POSITION
    }

    /**
     * A Box provider request and the output it produces, if it directly produces one.
     *
     * <p>Folder discovery can emit several selected resource types or only discover descendants.
     */
    enum Stage {
        EVENTS(BoxResource.EVENTS, Paging.EVENT_POSITION),
        SIGN_TEMPLATES(BoxResource.SIGN_TEMPLATES, Paging.MARKER),
        COLLECTIONS(BoxResource.COLLECTIONS, Paging.OFFSET),
        COLLECTION_ITEMS(BoxResource.COLLECTION_ITEMS, Paging.OFFSET),
        SIGN_REQUEST(BoxResource.SIGN_REQUEST, Paging.MARKER),
        ADMIN_LOGS(BoxResource.ADMIN_LOGS, Paging.EVENT_POSITION),
        ROOT_ITEMS("root_items", Paging.MARKER),
        FILE_COLLABORATIONS(BoxResource.FILE_COLLABORATIONS, Paging.MARKER),
        FOLDER_COLLABORATIONS(BoxResource.FOLDER_COLLABORATIONS, Paging.MARKER),
        FILE_COMMENTS(BoxResource.FILE_COMMENTS, Paging.OFFSET),
        RECENT_ITEMS(BoxResource.RECENT_ITEMS, Paging.MARKER),
        FILE_TASKS(BoxResource.FILE_TASKS, Paging.NONE),
        TRASHED_ITEMS(BoxResource.TRASHED_ITEMS, Paging.MARKER),
        USERS(BoxResource.USERS, Paging.MARKER),
        EXTRACT_FOLDER("extract_folder", Paging.MARKER),
        TEXT(BoxResource.TEXT_REPRESENTATION, Paging.NONE),
        ASK(BoxResource.AI_ASK, Paging.NONE),
        EXTRACT(BoxResource.AI_EXTRACT, Paging.NONE),
        STRUCTURED(BoxResource.AI_EXTRACT_STRUCTURED, Paging.NONE);

        final BoxResource output;
        final String operation;
        final Paging paging;

        Stage(BoxResource output, Paging paging) {
            this.output = output;
            operation = output.name;
            this.paging = paging;
        }

        Stage(String operation, Paging paging) {
            output = null;
            this.operation = operation;
            this.paging = paging;
        }

        boolean fileExtraction() {
            return this == TEXT || this == ASK || this == EXTRACT || this == STRUCTURED;
        }
    }

    final Stage stage;
    final String owner;
    final String file;
    final int depth;
    final int offset;
    final String position;
    final int pages;
    // Per-parent fingerprints make a cyclic token fail without storing response payloads.
    final Set<String> cursors;

    BoxWork(Stage stage, String owner, String file, int depth) {
        this(stage, owner, file, depth, 0, null, 0, Set.of());
    }

    private BoxWork(
            Stage stage,
            String owner,
            String file,
            int depth,
            int offset,
            String position,
            int pages,
            Set<String> cursors) {
        this.stage = stage;
        this.owner = owner;
        this.file = file;
        this.depth = depth;
        this.offset = offset;
        this.position = position;
        this.pages = pages;
        this.cursors = cursors;
    }

    /**
     * Creates fixed root operations for selected outputs and implied discovery.
     *
     * <p>Only one root folder listing is needed for ordinary files and folders.
     */
    static List<BoxWork> start(BoxConfig config) {
        List<BoxWork> plan = new ArrayList<>();
        if (config.includes(BoxResource.EVENTS)) {
            plan.add(
                    new BoxWork(Stage.EVENTS, null, null, 0, 0, config.eventPosition, 0, Set.of()));
        }
        add(plan, config, Stage.SIGN_TEMPLATES);
        if (config.includes(BoxResource.COLLECTIONS)
                || config.includes(BoxResource.COLLECTION_ITEMS)) {
            plan.add(new BoxWork(Stage.COLLECTIONS, null, null, 0));
        }
        add(plan, config, Stage.SIGN_REQUEST);
        if (config.includes(BoxResource.ADMIN_LOGS)) {
            plan.add(
                    new BoxWork(
                            Stage.ADMIN_LOGS, null, null, 0, 0, config.adminPosition, 0, Set.of()));
        }
        if (needsRootItems(config)) {
            plan.add(new BoxWork(Stage.ROOT_ITEMS, "0", null, 0));
        }
        add(plan, config, Stage.RECENT_ITEMS);
        add(plan, config, Stage.TRASHED_ITEMS);
        add(plan, config, Stage.USERS);
        if (extractionStages().stream().anyMatch(stage -> config.includes(stage.output))) {
            plan.add(new BoxWork(Stage.EXTRACT_FOLDER, config.folderId, null, 0));
        }
        return List.copyOf(plan);
    }

    static List<Stage> extractionStages() {
        return List.of(Stage.TEXT, Stage.ASK, Stage.EXTRACT, Stage.STRUCTURED);
    }

    private static void add(List<BoxWork> plan, BoxConfig config, Stage stage) {
        if (config.includes(stage.output)) {
            plan.add(new BoxWork(stage, null, null, 0));
        }
    }

    private static boolean needsRootItems(BoxConfig config) {
        return config.includes(BoxResource.FILES)
                || config.includes(BoxResource.FOLDERS)
                || config.includes(BoxResource.FILE_COLLABORATIONS)
                || config.includes(BoxResource.FOLDER_COLLABORATIONS)
                || config.includes(BoxResource.FILE_COMMENTS)
                || config.includes(BoxResource.FILE_TASKS);
    }

    BoxWork atOffset(int next, String pageIdentity) {
        Set<String> seen = history(pageIdentity);
        return new BoxWork(stage, owner, file, depth, next, null, pages + 1, Set.copyOf(seen));
    }

    BoxWork atPosition(String next, String pageIdentity) throws BoxReadException {
        String hash = "position:" + BoxConfig.digest(next);
        if (next.equals(position) || cursors.contains(hash)) {
            throw failure("provider repeated a continuation position");
        }
        Set<String> seen = history(pageIdentity);
        if (position != null) {
            seen.add("position:" + BoxConfig.digest(position));
        }
        seen.add(hash);
        return new BoxWork(stage, owner, file, depth, offset, next, pages + 1, Set.copyOf(seen));
    }

    private Set<String> history(String pageIdentity) {
        Set<String> seen = new HashSet<>(cursors);
        if (pageIdentity != null) {
            seen.add(pageIdentity);
        }
        return seen;
    }

    /**
     * Builds the documented Box request for this resource.
     *
     * <p>Marker mode is requested explicitly only on endpoints with an offset alternative.
     */
    HttpUrl url(HttpUrl api, BoxConfig config) {
        HttpUrl.Builder target = api.newBuilder().addPathSegment("2.0");
        switch (stage) {
            case EVENTS:
            case ADMIN_LOGS:
                target.addPathSegment("events")
                        .addQueryParameter(
                                "stream_type", stage == Stage.EVENTS ? "all" : "admin_logs")
                        .addQueryParameter("stream_position", position);
                if (stage == Stage.ADMIN_LOGS) {
                    if (config.adminAfter != null) {
                        target.addQueryParameter("created_after", config.adminAfter);
                    }
                    if (config.adminBefore != null) {
                        target.addQueryParameter("created_before", config.adminBefore);
                    }
                }
                break;
            case SIGN_TEMPLATES:
            case COLLECTIONS:
            case RECENT_ITEMS:
            case USERS:
                target.addPathSegment(stage.output.name);
                break;
            case SIGN_REQUEST:
                target.addPathSegment("sign_requests");
                break;
            case COLLECTION_ITEMS:
                path(target, "collections", owner, "items");
                break;
            case ROOT_ITEMS:
            case EXTRACT_FOLDER:
                path(target, "folders", owner, "items");
                break;
            case FILE_COLLABORATIONS:
                path(target, "files", owner, "collaborations");
                break;
            case FOLDER_COLLABORATIONS:
                path(target, "folders", owner, "collaborations");
                break;
            case FILE_COMMENTS:
                path(target, "files", owner, "comments");
                break;
            case FILE_TASKS:
                path(target, "files", owner, "tasks");
                break;
            case TRASHED_ITEMS:
                path(target, "folders", "trash", "items");
                break;
            default:
                throw new IllegalStateException("The Box operation has no list route");
        }
        if (stage == Stage.EXTRACT_FOLDER
                || stage == Stage.ROOT_ITEMS
                || stage == Stage.TRASHED_ITEMS
                || stage == Stage.USERS) {
            target.addQueryParameter("usemarker", "true");
        }
        if (stage.paging == Paging.OFFSET) {
            target.addQueryParameter("offset", Integer.toString(offset));
        } else if (stage.paging == Paging.MARKER && position != null) {
            target.addQueryParameter("marker", position);
        }
        if (stage.paging != Paging.NONE) {
            target.addQueryParameter("limit", Integer.toString(config.pageSize));
        }
        return target.build();
    }

    private static void path(HttpUrl.Builder target, String first, String second, String third) {
        target.addPathSegment(first).addPathSegment(second).addPathSegment(third);
    }

    BoxReadException failure(String reason) {
        return new BoxReadException(stage.operation, reason);
    }
}
