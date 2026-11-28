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
package org.apache.seatunnel.connectors.seatunnel.braintree;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeResource.Mode;
import org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSplit.Window;
import org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeXml.Element;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Read-only Braintree gateway operations for all seven native resources.
 *
 * <p>Each operation validates the expected XML envelope and collection metadata before advancing
 * its page, ID batch or search window.
 */
final class BraintreeGateway {
    private final BraintreeConfig config;
    private final BraintreeHttp http;

    BraintreeGateway(BraintreeConfig config, BraintreeHttp http) {
        this.config = config;
        this.http = http;
    }

    IdSet searchIds(BraintreeResource resource, Window window, long deadline)
            throws InterruptedException {
        Element root =
                http.read(
                        resource,
                        "/" + resource.name + "/advanced_search_ids",
                        search(resource, window, List.of()),
                        deadline);
        requireRoot(resource, root, "search-results");
        int size = pageSize(resource, root);
        Element collection = root.single("ids", resource);
        if (!collection.value.toString().isBlank()
                || collection.children.size() > resource.providerLimit) {
            throw resource.failure("invalid or oversized search ID collection");
        }
        List<String> ids = new ArrayList<>(collection.children.size());
        Set<String> distinct = new HashSet<>();
        for (Element item : collection.children) {
            if (!item.name.equals("item") || item.nil || !item.children.isEmpty()) {
                throw resource.failure("invalid search ID element");
            }
            String id = id(resource, item.value.toString());
            if (!distinct.add(id)) {
                throw resource.failure("duplicate IDs in search response");
            }
            ids.add(id);
        }
        return new IdSet(ids, Math.min(size, config.batchLimit));
    }

    Page list(BraintreeResource resource, Window window, int page, List<String> ids, long deadline)
            throws InterruptedException {
        String target = "/" + resource.name;
        if (resource.mode == Mode.IDS || resource.daily()) {
            target += "/advanced_search";
        }
        if (resource.mode == Mode.PAGED) {
            target += "?page=" + page;
        }
        Element root =
                http.read(
                        resource,
                        target,
                        resource.dated() ? search(resource, window, ids) : null,
                        deadline);
        requireRoot(resource, root, resource.root);
        int total = -1;
        int size = 0;
        if (resource.mode == Mode.PAGED) {
            total = root.number("total-items", resource);
            size = pageSize(resource, root);
            if (resource.daily()
                    && page == 1
                    && total >= Math.min(resource.providerLimit, config.resultLimit)) {
                return new Page(List.of(), total, size, null, true);
            }
        }
        List<Row> rows = records(resource, root);
        if (!ids.isEmpty()) {
            rows = orderByRequestedIds(resource, rows, ids);
        }
        if (resource.mode == Mode.PAGED) {
            long remaining = total - ((long) page - 1) * size;
            if (remaining < 0 || rows.size() != Math.min(remaining, size)) {
                throw resource.failure(
                        "paged record count does not match total-items and page-size");
            }
        }
        String digest = digest(resource, rows, total, size);
        return new Page(rows, total, size, digest, false);
    }

    private List<Row> records(BraintreeResource resource, Element root) {
        List<Row> result = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        int bytes = 0;
        for (Element child : root.children) {
            if (child.name.equals("errors") || child.name.equals("api-error-response")) {
                throw resource.failure("gateway returned an XML error instead of records");
            }
            if (!child.name.equals(resource.element)) {
                continue;
            }
            ObjectNode record = BraintreeXml.record(child, resource);
            JsonNode identifier = record.path("id");
            if (!identifier.isTextual()) {
                throw resource.failure("record has no valid ID");
            }
            String id = id(resource, identifier.textValue());
            if (!ids.add(id)) {
                throw resource.failure("duplicate record IDs in one response");
            }
            Instant timestamp = time(resource, record);
            String cursor = timestamp == null ? null : record.path(resource.cursor).asText();
            String json = BraintreeXml.encode(record, config.bytes, resource);
            bytes += json.getBytes(StandardCharsets.UTF_8).length;
            if (bytes > config.bytes) {
                throw resource.failure("projected XML response exceeds max_response_bytes");
            }
            result.add(new Row(id, cursor, timestamp, json));
        }
        return result;
    }

    private static List<Row> orderByRequestedIds(
            BraintreeResource resource, List<Row> rows, List<String> ids) {
        Map<String, Row> available = new HashMap<>();
        rows.forEach(row -> available.put(row.id, row));
        List<Row> ordered = new ArrayList<>(rows.size());
        for (String id : ids) {
            Row found = available.remove(id);
            if (found != null) {
                ordered.add(found);
            }
        }
        if (!available.isEmpty()) {
            throw resource.failure("gateway returned IDs outside the requested batch");
        }
        return ordered;
    }

    private static Instant time(BraintreeResource resource, ObjectNode record) {
        if (!resource.dated()) {
            return null;
        }
        JsonNode value = record.path(resource.cursor);
        if (!value.isTextual() || value.textValue().isBlank()) {
            throw resource.failure("record is missing its search cursor");
        }
        if (resource.daily()) {
            try {
                return LocalDate.parse(value.textValue()).atStartOfDay().toInstant(ZoneOffset.UTC);
            } catch (DateTimeException ex) {
                throw resource.failure("invalid dispute received_date");
            }
        }
        return BraintreeXml.instant(value.textValue(), resource);
    }

    private static int pageSize(BraintreeResource resource, Element root) {
        int size = root.number("page-size", resource);
        if (size < 1 || size > 1000) {
            throw resource.failure("provider page-size is outside the supported range");
        }
        return size;
    }

    private static void requireRoot(BraintreeResource resource, Element root, String expected) {
        if (!root.name.equals(expected)
                || !root.value.toString().isBlank()
                || root.nil && !root.children.isEmpty()) {
            throw resource.failure("unexpected XML collection root");
        }
    }

    private static String id(BraintreeResource resource, String identifier) {
        if (identifier.isBlank()
                || identifier.length() > 512
                || identifier.codePoints().anyMatch(c -> Character.isISOControl(c))) {
            throw resource.failure("invalid opaque record ID");
        }
        return identifier;
    }

    private static String digest(BraintreeResource resource, List<Row> rows, int total, int size) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update((total + "/" + size + "\n").getBytes(StandardCharsets.US_ASCII));
            for (Row row : rows) {
                byte[] body = row.json.getBytes(StandardCharsets.UTF_8);
                digest.update(Integer.toString(body.length).getBytes(StandardCharsets.US_ASCII));
                digest.update((byte) ':');
                digest.update(body);
            }
            return BraintreeConfig.hex(digest.digest());
        } catch (NoSuchAlgorithmException ex) {
            throw resource.failure("SHA-256 is unavailable");
        }
    }

    /**
     * Encodes the same inclusive date criteria for ID discovery and for item fetches.
     *
     * <p>Dates use the SDK's UTC Calendar format. Opaque IDs are escaped as XML text.
     */
    private static String search(BraintreeResource resource, Window window, List<String> ids) {
        StringWriter body = new StringWriter();
        try {
            XMLStreamWriter xml = XMLOutputFactory.newDefaultFactory().createXMLStreamWriter(body);
            xml.writeStartElement("search");
            xml.writeStartElement(resource.cursor);
            bound(xml, "min", window.min(resource.daily()));
            bound(xml, "max", window.max(resource.daily()));
            xml.writeEndElement();
            if (!ids.isEmpty()) {
                xml.writeStartElement("ids");
                xml.writeAttribute("type", "array");
                for (String id : ids) {
                    xml.writeStartElement("item");
                    xml.writeCharacters(id);
                    xml.writeEndElement();
                }
                xml.writeEndElement();
            }
            xml.writeEndElement();
            xml.close();
            return body.toString();
        } catch (XMLStreamException ex) {
            throw resource.failure("could not construct the search request");
        }
    }

    private static void bound(XMLStreamWriter xml, String name, Instant value)
            throws XMLStreamException {
        xml.writeStartElement(name);
        xml.writeAttribute("type", "datetime");
        xml.writeCharacters(value.toString());
        xml.writeEndElement();
    }

    /**
     * Ordered ID result and provider-approved fetch batch size.
     *
     * <p>A cap hit causes the whole search window to be divided before any fetch.
     */
    static final class IdSet {
        final List<String> ids;
        final int batchSize;

        IdSet(List<String> ids, int batchSize) {
            this.ids = List.copyOf(ids);
            this.batchSize = batchSize;
        }
    }

    /**
     * Validated page or batch with its fingerprint and native continuation metadata.
     *
     * <p>Saturated dispute responses carry metadata only and are discarded during subdivision.
     */
    static final class Page {
        final List<Row> rows;
        final int total;
        final int size;
        final String digest;
        final boolean saturated;

        Page(List<Row> rows, int total, int size, String digest, boolean saturated) {
            this.rows = List.copyOf(rows);
            this.total = total;
            this.size = size;
            this.digest = digest;
            this.saturated = saturated;
        }
    }

    /**
     * One validated row ready for time filtering and emission.
     *
     * <p>Its opaque identity and encoded record are kept together throughout replay.
     */
    static final class Row {
        final String id;
        final String cursor;
        final Instant timestamp;
        final String json;

        Row(String id, String cursor, Instant timestamp, String json) {
            this.id = id;
            this.cursor = cursor;
            this.timestamp = timestamp;
            this.json = json;
        }
    }
}
