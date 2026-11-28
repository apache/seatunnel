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

package org.apache.seatunnel.connectors.seatunnel.coap;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** XML records selected by a local XPath, with entity expansion and external access disabled. */
final class CoapXml {
    private CoapXml() {}

    static CoapPayload.Decoder decoder(CoapSettings.Listener settings, CatalogTable table) {
        String expression = settings.options.get(CoapOptions.XML_PATH);
        boolean keepRoot = settings.options.get(CoapOptions.XML_PRESERVE_ROOT);
        CoapParsedRows decode = new CoapParsedRows(settings, table);
        if (expression.isEmpty() || expression.length() > 512) {
            throw CoapSettings.invalid("xml_record_path must contain 1 to 512 characters");
        }
        try {
            XPathFactory xpath = XPathFactory.newInstance();
            xpath.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            XPath selectionContext = xpath.newXPath();
            Map<String, String> namespaces = settings.options.get(CoapOptions.XML_NAMESPACES);
            if (namespaces.size() > 32
                    || namespaces.entrySet().stream()
                            .anyMatch(
                                    entry ->
                                            entry.getKey().isEmpty()
                                                    || entry.getValue().isEmpty()
                                                    || entry.getValue().length() > 512)) {
                throw CoapSettings.invalid("xml_namespaces exceeds its prefix and URI limits");
            }
            selectionContext.setNamespaceContext(new Namespaces(namespaces));
            XPathExpression selection = selectionContext.compile(expression);
            DocumentBuilderFactory factory = xmlFactory();
            return (body, peer) -> {
                try {
                    DocumentBuilder builder = factory.newDocumentBuilder();
                    // Invalid caller content must not be written to stderr by the XML parser.
                    builder.setErrorHandler(
                            new DefaultHandler() {
                                @Override
                                public void error(SAXParseException error) throws SAXException {
                                    throw error;
                                }

                                @Override
                                public void fatalError(SAXParseException error)
                                        throws SAXException {
                                    throw error;
                                }
                            });
                    NodeList records =
                            (NodeList)
                                    selection.evaluate(
                                            builder.parse(new ByteArrayInputStream(body)),
                                            XPathConstants.NODESET);
                    if (records.getLength() == 0 || records.getLength() > CoapSettings.MAX_ROWS) {
                        throw new IOException("XML record selection is empty or exceeds its limit");
                    }
                    List<SeaTunnelRow> output = new ArrayList<>(records.getLength());
                    for (int i = 0; i < records.getLength(); i++) {
                        if (!(records.item(i) instanceof Element)) {
                            throw new IOException("XML record selection must contain elements");
                        }
                        Element element = (Element) records.item(i);
                        JsonNode value = element(element, 0);
                        if (keepRoot || !value.isObject()) {
                            value =
                                    CoapPayload.JSON
                                            .createObjectNode()
                                            .set(element.getTagName(), value);
                        }
                        output.add(decode.row(value));
                    }
                    return output;
                } catch (ParserConfigurationException | XPathExpressionException | SAXException e) {
                    throw new IOException("invalid XML payload");
                }
            };
        } catch (Exception e) {
            throw CoapSettings.invalid("invalid or unsupported XML configuration");
        }
    }

    private static DocumentBuilderFactory xmlFactory() throws ParserConfigurationException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);
        return factory;
    }

    private static JsonNode element(Element element, int depth) throws IOException {
        if (depth > 100) {
            throw new IOException("XML nesting exceeds the parser limit");
        }
        ObjectNode result = CoapPayload.JSON.createObjectNode();
        NamedNodeMap attributes = element.getAttributes();
        for (int i = 0; i < attributes.getLength(); i++) {
            Node attribute = attributes.item(i);
            result.put("@" + attribute.getNodeName(), attribute.getNodeValue());
        }
        StringBuilder text = new StringBuilder();
        for (Node child = element.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child instanceof Element) {
                append(result, child.getNodeName(), element((Element) child, depth + 1));
            } else if (child.getNodeType() == Node.TEXT_NODE
                    || child.getNodeType() == Node.CDATA_SECTION_NODE) {
                text.append(child.getNodeValue());
            }
        }
        if (result.isEmpty()) {
            return CoapPayload.JSON.getNodeFactory().textNode(text.toString());
        }
        if (!text.toString().trim().isEmpty()) {
            result.put("$text", text.toString());
        }
        return result;
    }

    private static void append(ObjectNode target, String name, JsonNode value) {
        JsonNode existing = target.get(name);
        if (existing == null) {
            target.set(name, value);
        } else if (existing.isArray()) {
            ((ArrayNode) existing).add(value);
        } else {
            target.putArray(name).add(existing).add(value);
        }
    }

    /** Resolves configured XPath prefixes locally without consulting the payload or network. */
    private static final class Namespaces implements NamespaceContext {
        private final Map<String, String> prefixes;

        private Namespaces(Map<String, String> prefixes) {
            this.prefixes = prefixes;
        }

        @Override
        public String getNamespaceURI(String prefix) {
            if (XMLConstants.XML_NS_PREFIX.equals(prefix)) {
                return XMLConstants.XML_NS_URI;
            }
            if (XMLConstants.XMLNS_ATTRIBUTE.equals(prefix)) {
                return XMLConstants.XMLNS_ATTRIBUTE_NS_URI;
            }
            return prefixes.getOrDefault(prefix, XMLConstants.NULL_NS_URI);
        }

        @Override
        public String getPrefix(String namespaceURI) {
            return prefixes.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(namespaceURI))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);
        }

        @Override
        public Iterator<String> getPrefixes(String namespaceURI) {
            return prefixes.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(namespaceURI))
                    .map(Map.Entry::getKey)
                    .iterator();
        }
    }
}
