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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.NullNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.TextNode;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Parses the two SP-API XML layouts and returns their defined record subtrees.
 *
 * <p>The parser never resolves external entities. Document depth, nodes, attributes and text are
 * bounded before they can reach a JSON record.
 */
final class SpXml {
    private static final int MAX_DEPTH = 64;
    private static final int MAX_NODES = 500000;
    private static final int MAX_VALUE = 1024 * 1024;
    private static final Set<String> BROWSE_ARRAYS = Set.of("attribute", "id", "refinementField");

    private final SpSettings settings;
    private final boolean browse;
    private int nodes;

    SpXml(SpSettings settings) {
        this.settings = settings;
        browse = settings.resource.format == SpResource.Format.BROWSE_XML;
    }

    List<ObjectNode> rows(String document) throws SpFailure {
        List<ObjectNode> result = new ArrayList<>();
        if (document.isBlank()) {
            return result;
        }
        XMLStreamReader reader = null;
        try {
            XMLInputFactory factory = XMLInputFactory.newFactory();
            factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
            factory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setXMLResolver(
                    (publicId, systemId, baseUri, namespace) -> {
                        throw new XMLStreamException("external XML resources are disabled");
                    });
            reader =
                    factory.createXMLStreamReader(
                            new StringReader(
                                    document.startsWith("\ufeff")
                                            ? document.substring(1)
                                            : document));
            List<String> path = new ArrayList<>();
            List<String> target =
                    browse
                            ? List.of("Result", "Node")
                            : List.of("AmazonEnvelope", "Message", "OrderReport");
            boolean seenRoot = false;
            while (reader.hasNext()) {
                int event = event(reader);
                if (event == XMLStreamConstants.START_ELEMENT) {
                    count(path.size() + 1);
                    String name = reader.getLocalName();
                    if (path.isEmpty()) {
                        if (seenRoot || !target.get(0).equals(name)) {
                            throw failure("unexpected XML report root");
                        }
                        seenRoot = true;
                    }
                    path.add(name);
                    if (target.equals(path)) {
                        if (result.size() >= settings.maxRows) {
                            throw failure("report exceeds max_report_rows");
                        }
                        JsonNode record = value(reader, path.size());
                        if (!record.isObject()) {
                            throw failure("expected structured XML report record");
                        }
                        result.add((ObjectNode) record);
                        path.remove(path.size() - 1);
                    }
                } else if (event == XMLStreamConstants.END_ELEMENT) {
                    path.remove(path.size() - 1);
                }
            }
            if (!seenRoot) {
                throw failure("missing XML report root");
            }
            return result;
        } catch (XMLStreamException | IllegalArgumentException e) {
            throw failure("invalid XML document or forbidden XML resource");
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (XMLStreamException ignored) {
                    // The document is already in a bounded in-memory buffer.
                }
            }
        }
    }

    private static int event(XMLStreamReader reader) throws XMLStreamException, SpFailure {
        if (Thread.currentThread().isInterrupted()) {
            throw new SpFailure("report XML", "XML decoding interrupted");
        }
        int type = reader.next();
        if (type == XMLStreamConstants.DTD || type == XMLStreamConstants.ENTITY_REFERENCE) {
            throw new SpFailure("report XML", "DTD and entity declarations are forbidden");
        }
        return type;
    }

    private JsonNode value(XMLStreamReader reader, int depth) throws XMLStreamException, SpFailure {
        String element = reader.getLocalName();
        ObjectNode object = SpJson.JSON.createObjectNode();
        if (reader.getAttributeCount() > 128) {
            throw failure("XML record has too many attributes");
        }
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String key = reader.getAttributeLocalName(i);
            String value = reader.getAttributeValue(i);
            if (value.length() > MAX_VALUE || object.has(key)) {
                throw failure("ambiguous or oversized XML attribute");
            }
            object.put(key, value);
        }
        StringBuilder text = new StringBuilder();
        while (reader.hasNext()) {
            int event = event(reader);
            if (event == XMLStreamConstants.START_ELEMENT) {
                count(depth + 1);
                String name = reader.getLocalName();
                add(object, name, value(reader, depth + 1));
            } else if (event == XMLStreamConstants.CHARACTERS
                    || event == XMLStreamConstants.CDATA) {
                text.append(reader.getText());
                if (text.length() > MAX_VALUE) {
                    throw failure("XML record text exceeds its size limit");
                }
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                String content = text.toString().strip();
                if (object.isEmpty()) {
                    return content.isEmpty() ? NullNode.getInstance() : scalar(element, content);
                }
                String key = browse ? "text" : "value";
                if (!content.isEmpty()) {
                    if (object.has(key)) {
                        throw failure("mixed XML content conflicts with a record field");
                    }
                    object.set(key, scalar(element, content));
                }
                return object;
            }
        }
        throw failure("incomplete XML record");
    }

    private JsonNode scalar(String element, String text) throws SpFailure {
        if (!browse && ("Quantity".equals(element) || "Amount".equals(element))) {
            try {
                return SpJson.JSON.getNodeFactory().numberNode(new BigDecimal(text));
            } catch (NumberFormatException e) {
                throw failure("invalid numeric field in an order record");
            }
        }
        return TextNode.valueOf(text);
    }

    private void add(ObjectNode output, String field, JsonNode value) {
        JsonNode existing = output.get(field);
        if (existing == null) {
            if (browse && BROWSE_ARRAYS.contains(field)) {
                output.putArray(field).add(value);
            } else {
                output.set(field, value);
            }
        } else if (existing.isArray()) {
            ((ArrayNode) existing).add(value);
        } else {
            output.putArray(field).add(existing).add(value);
        }
    }

    private void count(int depth) throws SpFailure {
        if (depth > MAX_DEPTH || ++nodes > MAX_NODES) {
            throw failure("XML document exceeds its element or depth limit");
        }
    }

    private SpFailure failure(String reason) {
        return new SpFailure(settings.resource.name(), reason);
    }
}
