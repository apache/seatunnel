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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.box.BoxWork.Paging;
import org.apache.seatunnel.connectors.seatunnel.box.BoxWork.Stage;

import okhttp3.HttpUrl;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Validated Box data and the next committed position when its rows are published.
 *
 * <p>Child lookups, extraction results and continuation never advance the split before the source
 * collector has accepted the associated rows.
 */
final class BoxData {
    private static final int MAX_OFFSET = 9999;

    final List<SeaTunnelRow> rows;
    final String fingerprint;
    final BoxSplit next;

    private BoxData(List<SeaTunnelRow> rows, String fingerprint, BoxSplit next) {
        this.rows = rows;
        this.fingerprint = fingerprint;
        this.next = next;
    }

    /**
     * Fetches one pending read or per-file extraction and validates its entire result.
     *
     * <p>For ordinary lists, the full page fingerprint detects changes to a partial checkpoint. A
     * file extraction emits at most one row, so no derived model response is saved in state.
     */
    static BoxData read(
            BoxHttp client, BoxConfig config, BoxSplit state, String tableId, long deadline)
            throws BoxReadException, InterruptedException {
        BoxWork work = state.work.get(0);
        BoxHttp.Acquisition acquisition = client.start(state, deadline);
        if (work.stage.fileExtraction()) {
            SeaTunnelRow file = extract(client, acquisition, config, work, tableId);
            BoxHttp.deadline(work.stage.operation, deadline);
            return new BoxData(
                    List.of(file), null, state.advance(List.of(), acquisition.used(), 0, Set.of()));
        }

        JsonNode root = acquisition.get(work.url(client.api, config), work.stage.operation);
        JsonNode entries = root.path("entries");
        int maxEntries = work.stage.paging == Paging.NONE ? BoxSplit.MAX_PENDING : config.pageSize;
        if (!entries.isArray() || entries.size() > maxEntries) {
            throw work.failure("expected a bounded Box entries array");
        }
        String fingerprint = BoxConfig.digest(BoxJson.write(root, work.stage.operation));
        if (state.partialFingerprint != null && !state.partialFingerprint.equals(fingerprint)) {
            throw work.failure("provider page changed after checkpoint");
        }
        int count = work.stage == Stage.EXTRACT_FOLDER ? entries.size() : 0;
        if (count > config.maxItems - state.extractionItems) {
            throw work.failure("max_items exhausted during file discovery");
        }

        List<SeaTunnelRow> rows = new ArrayList<>();
        List<BoxWork> children = new ArrayList<>();
        Set<String> discovered = new HashSet<>();
        Set<String> pageIds = new TreeSet<>();
        for (JsonNode entry : entries) {
            BoxHttp.deadline(work.stage.operation, deadline);
            if (!entry.isObject()) {
                throw work.failure("Box entries array contains a non-object");
            }
            String id = identity(entry, work);
            if (work.stage.paging != Paging.EVENT_POSITION && !pageIds.add(BoxConfig.digest(id))) {
                throw work.failure("duplicate Box item in one parent page");
            }
            switch (work.stage) {
                case COLLECTIONS:
                    if (config.includes(BoxResource.COLLECTIONS)) {
                        rows.add(row(BoxResource.COLLECTIONS, id, null, entry, null, tableId));
                    }
                    if (config.includes(BoxResource.COLLECTION_ITEMS)) {
                        children.add(
                                new BoxWork(
                                        Stage.COLLECTION_ITEMS,
                                        decimalId(entry.path("id"), work),
                                        null,
                                        0));
                    }
                    break;
                case ROOT_ITEMS:
                    rootItem(config, entry, rows, children, work, tableId);
                    break;
                case EXTRACT_FOLDER:
                    extractItem(config, state, entry, children, discovered, work);
                    break;
                default:
                    String key =
                            work.stage == Stage.COLLECTION_ITEMS
                                            || work.stage == Stage.TRASHED_ITEMS
                                    ? itemId(entry.path("id"), work)
                                    : id;
                    rows.add(row(work.stage.output, key, work.owner, entry, null, tableId));
                    break;
            }
        }

        String identity =
                pageIds.isEmpty() ? null : "page:" + BoxConfig.digest(String.join("\n", pageIds));
        if (identity != null && work.cursors.contains(identity)) {
            throw work.failure("provider repeated a page for the same parent");
        }
        BoxWork next = continuation(root, entries.size(), work, config, identity);
        if (next != null) {
            if (next.pages >= config.maxPages) {
                throw work.failure("max_pages_per_parent exhausted before terminal position");
            }
            children.add(next);
        }
        BoxSplit advanced = state.advance(children, acquisition.used(), count, discovered);
        BoxHttp.deadline(work.stage.operation, deadline);
        return new BoxData(rows, fingerprint, advanced);
    }

    private static BoxWork continuation(
            JsonNode root, int count, BoxWork work, BoxConfig config, String pageIdentity)
            throws BoxReadException {
        switch (work.stage.paging) {
            case EVENT_POSITION:
                if (intValue(root.path("chunk_size")) != count) {
                    throw work.failure("Box event chunk size contradicts entries");
                }
                if (count == 0) {
                    return null;
                }
                String position = cursor(root.path("next_stream_position"), work, true);
                if (position == null) {
                    throw work.failure("nonempty Box event chunk has no stream position");
                }
                return work.atPosition(position, null);
            case OFFSET:
                long offset = intValue(root.path("offset"));
                long limit = intValue(root.path("limit"));
                long total = intValue(root.path("total_count"));
                if (offset != work.offset
                        || limit < 1
                        || limit > config.pageSize
                        || total < 0
                        || count > limit
                        || (count > 0 && (offset >= total || count > total - offset))) {
                    throw work.failure("contradictory Box offset, limit or total_count");
                }
                long next = offset + limit;
                if (next >= total) {
                    return null;
                }
                if (next > MAX_OFFSET) {
                    throw work.failure(
                            "Box offset limit of 9999 reached before the result completed");
                }
                return work.atOffset((int) next, pageIdentity);
            case MARKER:
                String marker = cursor(root.path("next_marker"), work, false);
                return marker == null ? null : work.atPosition(marker, pageIdentity);
            default:
                return null;
        }
    }

    private static String cursor(JsonNode field, BoxWork work, boolean allowNumber)
            throws BoxReadException {
        if (field.isMissingNode() || field.isNull()) {
            return null;
        }
        if (field.isTextual() || (allowNumber && field.isIntegralNumber())) {
            String value = field.asText();
            if (value.isEmpty()) {
                return null;
            }
            if (!value.isBlank()
                    && value.length() <= 4096
                    && value.chars().noneMatch(Character::isISOControl)) {
                return value;
            }
        }
        throw work.failure("invalid Box continuation value");
    }

    private static long intValue(JsonNode value) {
        return value.isIntegralNumber() && value.canConvertToLong() ? value.longValue() : -1L;
    }

    private static void rootItem(
            BoxConfig config,
            JsonNode entry,
            List<SeaTunnelRow> rows,
            List<BoxWork> children,
            BoxWork work,
            String tableId)
            throws BoxReadException {
        String type = entry.path("type").asText();
        if ("file".equals(type)) {
            String id = decimalId(entry.path("id"), work);
            if (config.includes(BoxResource.FILES)) {
                rows.add(row(BoxResource.FILES, id, work.owner, entry, null, tableId));
            }
            for (Stage stage :
                    List.of(Stage.FILE_COLLABORATIONS, Stage.FILE_COMMENTS, Stage.FILE_TASKS)) {
                if (config.includes(stage.output)) {
                    children.add(new BoxWork(stage, id, null, 0));
                }
            }
        } else if ("folder".equals(type)) {
            String id = decimalId(entry.path("id"), work);
            if (config.includes(BoxResource.FOLDERS)) {
                rows.add(row(BoxResource.FOLDERS, id, work.owner, entry, null, tableId));
            }
            if (config.includes(BoxResource.FOLDER_COLLABORATIONS)) {
                children.add(new BoxWork(Stage.FOLDER_COLLABORATIONS, id, null, 0));
            }
        }
    }

    private static void extractItem(
            BoxConfig config,
            BoxSplit state,
            JsonNode entry,
            List<BoxWork> children,
            Set<String> discovered,
            BoxWork work)
            throws BoxReadException {
        String type = entry.path("type").asText();
        if ("file".equals(type)) {
            String id = decimalId(entry.path("id"), work);
            newItem(state, discovered, "file:" + id, work);
            for (Stage operation : BoxWork.extractionStages()) {
                if (config.includes(operation.output)) {
                    children.add(new BoxWork(operation, work.owner, id, work.depth));
                }
            }
        } else if ("folder".equals(type) && config.recursive) {
            if (work.depth == config.maxDepth) {
                throw work.failure("max_depth exhausted before visiting a nested folder");
            }
            String id = decimalId(entry.path("id"), work);
            newItem(state, discovered, "folder:" + id, work);
            children.add(new BoxWork(Stage.EXTRACT_FOLDER, id, null, work.depth + 1));
        }
    }

    private static void newItem(BoxSplit state, Set<String> discovered, String item, BoxWork work)
            throws BoxReadException {
        if (state.visitedItems.contains(item) || !discovered.add(item)) {
            throw work.failure("encountered a recursive cycle or repeated extraction item");
        }
    }

    private static SeaTunnelRow extract(
            BoxHttp client,
            BoxHttp.Acquisition acquisition,
            BoxConfig config,
            BoxWork work,
            String tableId)
            throws BoxReadException, InterruptedException {
        HttpUrl fileUrl =
                client.api
                        .newBuilder()
                        .addPathSegment("2.0")
                        .addPathSegment("files")
                        .addPathSegment(work.file)
                        .build();
        HttpUrl completeUrl =
                config.fileFields == null
                        ? fileUrl
                        : fileUrl.newBuilder()
                                .addQueryParameter("fields", config.fileFields)
                                .build();
        JsonNode file = acquisition.get(completeUrl, work.stage.operation);
        if (!"file".equals(file.path("type").asText())
                || !work.file.equals(decimalId(file.path("id"), work))) {
            throw work.failure("file detail does not match the discovered file");
        }
        JsonNode parent = file.path("parent").path("id");
        if (!parent.isMissingNode()
                && !parent.isNull()
                && !work.owner.equals(decimalId(parent, work))) {
            throw work.failure("file detail belongs to a different folder");
        }
        JsonNode version = file.path("file_version").path("id");
        String content =
                work.stage == Stage.TEXT
                        ? BoxDownload.text(
                                client,
                                acquisition,
                                config,
                                work,
                                fileUrl,
                                version.isMissingNode() || version.isNull()
                                        ? null
                                        : decimalId(version, work))
                        : ai(client, acquisition, config, work);
        return row(work.stage.output, work.file, work.owner, file, content, tableId);
    }

    private static String ai(
            BoxHttp client, BoxHttp.Acquisition acquisition, BoxConfig config, BoxWork work)
            throws BoxReadException, InterruptedException {
        ObjectNode request = BoxJson.MAPPER.createObjectNode();
        request.putArray("items").addObject().put("type", "file").put("id", work.file);
        String path;
        switch (work.stage) {
            case ASK:
                path = "ask";
                request.put("mode", "single_item_qa").put("prompt", config.ask);
                break;
            case EXTRACT:
                path = "extract";
                request.put("prompt", config.extract);
                break;
            case STRUCTURED:
                path = "extract_structured";
                request.set(
                        "fields",
                        BoxJson.read(
                                config.structured.getBytes(StandardCharsets.UTF_8),
                                work.stage.operation));
                break;
            default:
                throw new IllegalStateException("Not a Box AI operation");
        }
        HttpUrl url =
                client.api
                        .newBuilder()
                        .addPathSegment("2.0")
                        .addPathSegment("ai")
                        .addPathSegment(path)
                        .build();
        JsonNode response =
                acquisition.post(
                        url, BoxJson.write(request, work.stage.operation), work.stage.operation);
        JsonNode answer = response.path("answer");
        if (work.stage == Stage.STRUCTURED && answer.isObject()) {
            return BoxJson.write(response, work.stage.operation);
        }
        if (work.stage != Stage.STRUCTURED && answer.isTextual()) {
            return answer.textValue();
        }
        throw work.failure("Box AI response has no answer in the expected format");
    }

    private static String identity(JsonNode entry, BoxWork work) throws BoxReadException {
        switch (work.stage) {
            case EVENTS:
            case ADMIN_LOGS:
                return itemId(entry.path("event_id"), work);
            case RECENT_ITEMS:
                JsonNode item = entry.path("item");
                if (item.isMissingNode()) {
                    return itemId(entry.path("id"), work);
                }
                return itemId(item.path("type"), work) + ":" + itemId(item.path("id"), work);
            case ROOT_ITEMS:
            case EXTRACT_FOLDER:
            case COLLECTION_ITEMS:
            case TRASHED_ITEMS:
                return itemId(entry.path("type"), work) + ":" + itemId(entry.path("id"), work);
            default:
                return itemId(entry.path("id"), work);
        }
    }

    private static String itemId(JsonNode field, BoxWork work) throws BoxReadException {
        if (field.isTextual() || field.isIntegralNumber()) {
            String id = field.asText();
            if (!id.isBlank()
                    && id.length() <= 512
                    && id.chars().noneMatch(Character::isISOControl)) {
                return id;
            }
        }
        throw work.failure("record has no usable Box identity");
    }

    private static String decimalId(JsonNode field, BoxWork work) throws BoxReadException {
        String id = itemId(field, work);
        if (!id.matches("[0-9]{1,64}")) {
            throw work.failure("record has no decimal Box path ID");
        }
        return id;
    }

    private static SeaTunnelRow row(
            BoxResource resource,
            String id,
            String parent,
            JsonNode value,
            String content,
            String tableId)
            throws BoxReadException {
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            resource.name, id, parent, BoxJson.write(value, resource.name), content
                        });
        row.setTableId(tableId);
        return row;
    }
}
