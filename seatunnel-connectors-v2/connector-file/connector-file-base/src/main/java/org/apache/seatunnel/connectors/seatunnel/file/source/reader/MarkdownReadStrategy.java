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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.apache.commons.io.IOUtils;

import com.vladsch.flexmark.ast.BlockQuote;
import com.vladsch.flexmark.ast.BulletList;
import com.vladsch.flexmark.ast.Code;
import com.vladsch.flexmark.ast.FencedCodeBlock;
import com.vladsch.flexmark.ast.Heading;
import com.vladsch.flexmark.ast.Image;
import com.vladsch.flexmark.ast.Link;
import com.vladsch.flexmark.ast.ListItem;
import com.vladsch.flexmark.ast.OrderedList;
import com.vladsch.flexmark.ast.Paragraph;
import com.vladsch.flexmark.ast.ThematicBreak;
import com.vladsch.flexmark.ext.tables.TableBlock;
import com.vladsch.flexmark.ext.tables.TableCell;
import com.vladsch.flexmark.ext.tables.TableRow;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.ast.Node;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class MarkdownReadStrategy extends AbstractReadStrategy {

    private static final int DEFAULT_PAGE_NUMBER = 1;
    private static final int DEFAULT_POSITION = 1;
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
    private static final String[] DEFAULT_FIELD_NAMES = {
        "element_id",
        "element_type",
        "heading_level",
        "text",
        "page_number",
        "position_index",
        "parent_id",
        "child_ids"
    };
    private static final SeaTunnelDataType[] DEFAULT_FIELD_TYPES = {
        BasicType.STRING_TYPE,
        BasicType.STRING_TYPE,
        BasicType.INT_TYPE,
        BasicType.STRING_TYPE,
        BasicType.INT_TYPE,
        BasicType.INT_TYPE,
        BasicType.STRING_TYPE,
        BasicType.STRING_TYPE
    };
    /** Stable metadata fields appended for downstream RAG/document indexing pipelines. */
    private static final String[] RAG_METADATA_FIELD_NAMES = {
        "source_uri", "document_id", "chunk_id", "chunk_index", "content_hash"
    };

    private static final SeaTunnelDataType[] RAG_METADATA_FIELD_TYPES = {
        BasicType.STRING_TYPE,
        BasicType.STRING_TYPE,
        BasicType.STRING_TYPE,
        BasicType.INT_TYPE,
        BasicType.STRING_TYPE
    };

    private boolean markdownRagMetadataEnabled =
            FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.defaultValue();

    private static class NodeInfo {
        String elementId;
        String parentId;
        List<String> childIds = new ArrayList<>();
        int positionIndex;

        NodeInfo(String elementId, String parentId, int positionIndex) {
            this.elementId = elementId;
            this.parentId = parentId;
            this.positionIndex = positionIndex;
        }
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        String markdown;
        try (InputStream inputStream = hadoopFileSystemProxy.getInputStream(path)) {
            markdown = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
        Parser parser = Parser.builder().build();
        Node document = parser.parse(markdown);
        String sourceUri = normalizeSourceUri(path);

        Map<Node, NodeInfo> nodeInfoMap = new IdentityHashMap<>();
        Map<String, Integer> typeCounters = new HashMap<>();
        List<SeaTunnelRow> rows = new ArrayList<>();

        assignIdsAndCollectTree(document, null, nodeInfoMap, DEFAULT_POSITION, typeCounters);
        generateRows(
                document,
                rows,
                nodeInfoMap,
                DEFAULT_PAGE_NUMBER,
                sourceUri,
                buildDocumentId(sourceUri));

        for (SeaTunnelRow row : rows) {
            output.collect(row);
        }
    }

    private void assignIdsAndCollectTree(
            Node node,
            Node parent,
            Map<Node, NodeInfo> nodeInfoMap,
            int position,
            Map<String, Integer> typeCounters) {
        String elementType = node.getClass().getSimpleName();
        String elementId = null;

        if (isEligibleForRow(node)) {
            int count = typeCounters.getOrDefault(elementType, 0) + 1;
            typeCounters.put(elementType, count);
            elementId = elementType + "_" + count;
        }

        String parentId = parent == null ? null : nodeInfoMap.get(parent).elementId;
        NodeInfo nodeInfo = new NodeInfo(elementId, parentId, position);
        nodeInfoMap.put(node, nodeInfo);

        int childPosition = 1;
        for (Node child = node.getFirstChild(); child != null; child = child.getNext()) {
            assignIdsAndCollectTree(child, node, nodeInfoMap, childPosition++, typeCounters);
            NodeInfo childInfo = nodeInfoMap.get(child);
            if (childInfo.elementId != null) {
                nodeInfo.childIds.add(childInfo.elementId);
            }
        }
    }

    private void generateRows(
            Node node,
            List<SeaTunnelRow> rows,
            Map<Node, NodeInfo> nodeInfoMap,
            int pageNumber,
            String sourceUri,
            String documentId) {
        if (isEligibleForRow(node)) {
            NodeInfo nodeInfo = nodeInfoMap.get(node);
            String elementType = node.getClass().getSimpleName();
            Integer headingLevel = null;
            String text = extractValue(node);

            if (node instanceof Heading) {
                headingLevel = ((Heading) node).getLevel();
            }

            Object[] fields =
                    new Object[] {
                        nodeInfo.elementId,
                        elementType,
                        headingLevel,
                        text,
                        pageNumber,
                        nodeInfo.positionIndex,
                        nodeInfo.parentId,
                        nodeInfo.childIds.isEmpty() ? null : String.join(",", nodeInfo.childIds)
                    };
            if (markdownRagMetadataEnabled) {
                fields = appendRagMetadata(fields, sourceUri, documentId, rows.size() + 1, text);
            }

            rows.add(new SeaTunnelRow(fields));
            log.debug(
                    "Added row: element_id={} type={} heading_level={} text={} parent_id={} child_ids={}",
                    nodeInfo.elementId,
                    elementType,
                    headingLevel,
                    text,
                    nodeInfo.parentId,
                    nodeInfo.childIds);
        }

        for (Node child = node.getFirstChild(); child != null; child = child.getNext()) {
            generateRows(child, rows, nodeInfoMap, pageNumber, sourceUri, documentId);
        }
    }

    private boolean isEligibleForRow(Node node) {
        if (node instanceof Paragraph) {
            Node parent = node.getParent();
            if (parent instanceof ListItem || parent instanceof BlockQuote) {
                return false;
            }
        }

        return node instanceof Heading
                || node instanceof Paragraph
                || node instanceof ListItem
                || node instanceof BulletList
                || node instanceof OrderedList
                || node instanceof BlockQuote
                || node instanceof FencedCodeBlock
                || node instanceof TableBlock;
    }

    private String extractValue(Node node) {
        if (node instanceof ListItem) {
            return extractTextFromChildren(node);
        } else if (node instanceof Heading || node instanceof Paragraph) {
            return extractTextFromChildren(node);
        } else if (node instanceof BulletList) {
            return bulletListToString((BulletList) node);
        } else if (node instanceof OrderedList) {
            return orderedListToString((OrderedList) node);
        } else if (node instanceof Code) {
            return ((Code) node).getText().toString();
        } else if (node instanceof FencedCodeBlock) {
            return ((FencedCodeBlock) node).getContentChars().toString();
        } else if (node instanceof BlockQuote) {
            return extractTextFromChildren(node);
        } else if (node instanceof ThematicBreak) {
            return "---";
        } else if (node instanceof Link) {
            return ((Link) node).getUrl().toString();
        } else if (node instanceof Image) {
            return ((Image) node).getUrl().toString();
        } else if (node instanceof TableBlock) {
            return tableToString((TableBlock) node);
        }

        return node.getChars().toString();
    }

    private String extractTextFromChildren(Node node) {
        StringBuilder sb = new StringBuilder();
        for (Node child = node.getFirstChild(); child != null; child = child.getNext()) {
            sb.append(child.getChars());
        }

        return sb.toString().trim();
    }

    private String bulletListToString(BulletList list) {
        StringBuilder sb = new StringBuilder();
        for (Node item = list.getFirstChild(); item != null; item = item.getNext()) {
            if (item instanceof ListItem) {
                sb.append("- ").append(extractTextFromChildren(item)).append("\n");
            }
        }

        return sb.toString();
    }

    private String orderedListToString(OrderedList list) {
        StringBuilder sb = new StringBuilder();
        int num = 1;
        for (Node item = list.getFirstChild(); item != null; item = item.getNext()) {
            if (item instanceof ListItem) {
                sb.append(num++).append(". ").append(extractTextFromChildren(item)).append("\n");
            }
        }

        return sb.toString();
    }

    private String tableToString(TableBlock table) {
        StringBuilder sb = new StringBuilder();
        for (Node row = table.getFirstChild(); row != null; row = row.getNext()) {
            if (row instanceof TableRow) {
                for (Node cell = row.getFirstChild(); cell != null; cell = cell.getNext()) {
                    if (cell instanceof TableCell) {
                        sb.append(((TableCell) cell).getText().toString()).append(" | ");
                    }
                }
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        if (markdownRagMetadataEnabled) {
            return new SeaTunnelRowType(
                    concat(DEFAULT_FIELD_NAMES, RAG_METADATA_FIELD_NAMES),
                    concat(DEFAULT_FIELD_TYPES, RAG_METADATA_FIELD_TYPES));
        }
        return new SeaTunnelRowType(DEFAULT_FIELD_NAMES, DEFAULT_FIELD_TYPES);
    }

    @Override
    public void setPluginConfig(Config pluginConfig) {
        super.setPluginConfig(pluginConfig);
        if (pluginConfig.hasPath(FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.key())) {
            markdownRagMetadataEnabled =
                    pluginConfig.getBoolean(
                            FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.key());
        }
    }

    private Object[] appendRagMetadata(
            Object[] fields, String sourceUri, String documentId, int chunkIndex, String text) {
        String contentHash = sha256Hex(text == null ? "" : text);
        // Keep chunk ids stable across re-reads of the same logical document while still changing
        // when the chunk content changes.
        String chunkId = "chunk_" + sha256Hex(documentId + ":" + chunkIndex + ":" + contentHash);
        Object[] enriched = new Object[fields.length + RAG_METADATA_FIELD_NAMES.length];
        System.arraycopy(fields, 0, enriched, 0, fields.length);
        enriched[fields.length] = sourceUri;
        enriched[fields.length + 1] = documentId;
        enriched[fields.length + 2] = chunkId;
        enriched[fields.length + 3] = chunkIndex;
        enriched[fields.length + 4] = contentHash;
        return enriched;
    }

    private static String buildDocumentId(String sourceUri) {
        // Document ids stay anchored to the normalized source location so every chunk from the same
        // file shares one stable parent id.
        return "doc_" + sha256Hex(sourceUri);
    }

    private static String normalizeSourceUri(String sourceUri) {
        // Normalize local file URIs to the path form emitted by existing local-file reads so the
        // metadata contract stays stable between "file:/..." and plain local paths.
        if (!sourceUri.startsWith("file:")) {
            return sourceUri;
        }
        try {
            return Paths.get(URI.create(sourceUri)).toString();
        } catch (IllegalArgumentException e) {
            return sourceUri;
        }
    }

    private static String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            char[] chars = new char[bytes.length * 2];
            for (int i = 0; i < bytes.length; i++) {
                int unsigned = bytes[i] & 0xFF;
                chars[i * 2] = HEX_CHARS[unsigned >>> 4];
                chars[i * 2 + 1] = HEX_CHARS[unsigned & 0x0F];
            }
            return new String(chars);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private static String[] concat(String[] left, String[] right) {
        String[] result = new String[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    private static SeaTunnelDataType[] concat(SeaTunnelDataType[] left, SeaTunnelDataType[] right) {
        SeaTunnelDataType[] result = new SeaTunnelDataType[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }
}
