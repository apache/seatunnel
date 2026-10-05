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
import org.apache.seatunnel.api.table.type.KnowledgeSyncMetadataField;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.FileSourceDocumentRouting;
import org.apache.seatunnel.connectors.seatunnel.file.source.MarkdownKnowledgeSyncMetadata;

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
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class MarkdownReadStrategy extends AbstractReadStrategy {

    private static final int DEFAULT_PAGE_NUMBER = 1;
    private static final int DEFAULT_POSITION = 1;
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

    /** Per-file logical identity and digest reused by every row emitted from that document. */
    private static final class LogicalDocumentMetadata {
        private final String sourceUri;
        private final String documentId;
        private final String documentHash;

        private LogicalDocumentMetadata(String sourceUri, String documentId, String documentHash) {
            this.sourceUri = sourceUri;
            this.documentId = documentId;
            this.documentHash = documentHash;
        }
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        String logicalSourceUri = null;
        String logicalDocumentId = null;
        MessageDigest documentDigest = null;
        if (markdownRagMetadataEnabled) {
            logicalSourceUri = MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(path);
            logicalDocumentId = MarkdownKnowledgeSyncMetadata.buildDocumentId(logicalSourceUri);
            documentDigest = MarkdownKnowledgeSyncMetadata.newSha256Digest();
        }

        String markdown;
        try (InputStream inputStream = hadoopFileSystemProxy.getInputStream(path)) {
            InputStream contentStream = inputStream;
            if (documentDigest != null) {
                contentStream = new DigestInputStream(inputStream, documentDigest);
            }
            markdown = IOUtils.toString(contentStream, StandardCharsets.UTF_8);
        }
        LogicalDocumentMetadata logicalMetadata =
                documentDigest == null
                        ? null
                        : new LogicalDocumentMetadata(
                                logicalSourceUri,
                                logicalDocumentId,
                                MarkdownKnowledgeSyncMetadata.toLowerHex(documentDigest.digest()));
        collectMarkdownRows(markdown, path, logicalMetadata, output);
    }

    /**
     * Emits rows from converted Markdown while preserving the identity of the original source.
     *
     * <p>{@code sourcePath} and {@code sourceDocumentHash} must describe the original source file,
     * not an intermediate Markdown file or the converted Markdown bytes.
     */
    void collectMarkdownRows(
            String markdown,
            String sourcePath,
            String sourceDocumentHash,
            Collector<SeaTunnelRow> output) {
        validateConvertedMarkdown(markdown, sourcePath, sourceDocumentHash);
        LogicalDocumentMetadata logicalMetadata = null;
        if (markdownRagMetadataEnabled) {
            String logicalSourceUri =
                    MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(sourcePath);
            logicalMetadata =
                    new LogicalDocumentMetadata(
                            logicalSourceUri,
                            MarkdownKnowledgeSyncMetadata.buildDocumentId(logicalSourceUri),
                            sourceDocumentHash);
        }
        collectMarkdownRows(markdown, sourcePath, logicalMetadata, output);
    }

    private void validateConvertedMarkdown(
            String markdown, String sourcePath, String sourceDocumentHash) {
        if (sourcePath == null || sourcePath.trim().isEmpty()) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.DATA_DESERIALIZE_FAILED,
                    "Cannot parse converted Markdown without the original source path");
        }
        if (markdown == null || markdown.trim().isEmpty()) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.DATA_DESERIALIZE_FAILED,
                    "Converted Markdown is empty for source: " + sourcePath);
        }
        if (markdownRagMetadataEnabled
                && (sourceDocumentHash == null || sourceDocumentHash.trim().isEmpty())) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.DATA_DESERIALIZE_FAILED,
                    "Original document hash is missing for source: " + sourcePath);
        }
    }

    private void collectMarkdownRows(
            String markdown,
            String sourcePath,
            LogicalDocumentMetadata logicalMetadata,
            Collector<SeaTunnelRow> output) {
        Parser parser = Parser.builder().build();
        Node document = parser.parse(markdown);
        String sourceUri = FileSourceDocumentRouting.normalizeSourceUri(sourcePath);

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
                FileSourceDocumentRouting.buildDocumentId(sourceUri),
                logicalMetadata);

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
            String documentId,
            LogicalDocumentMetadata logicalMetadata) {
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
            String contentHash = null;
            if (markdownRagMetadataEnabled) {
                contentHash = FileSourceDocumentRouting.sha256Hex(text == null ? "" : text);
                fields =
                        appendRagMetadata(
                                fields, sourceUri, documentId, rows.size() + 1, contentHash);
            }

            SeaTunnelRow row = new SeaTunnelRow(fields);
            if (logicalMetadata != null) {
                addKnowledgeSyncMetadata(
                        row,
                        logicalMetadata.sourceUri,
                        logicalMetadata.documentId,
                        logicalMetadata.documentHash,
                        contentHash);
            }
            rows.add(row);
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
            generateRows(
                    child, rows, nodeInfoMap, pageNumber, sourceUri, documentId, logicalMetadata);
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
            Object[] fields,
            String sourceUri,
            String documentId,
            int chunkIndex,
            String contentHash) {
        // Keep chunk ids stable across re-reads of the same logical document while still changing
        // when the chunk content changes.
        String chunkId =
                "chunk_"
                        + FileSourceDocumentRouting.sha256Hex(
                                documentId + ":" + chunkIndex + ":" + contentHash);
        Object[] enriched = new Object[fields.length + RAG_METADATA_FIELD_NAMES.length];
        System.arraycopy(fields, 0, enriched, 0, fields.length);
        enriched[fields.length] = sourceUri;
        enriched[fields.length + 1] = documentId;
        enriched[fields.length + 2] = chunkId;
        enriched[fields.length + 3] = chunkIndex;
        enriched[fields.length + 4] = contentHash;
        return enriched;
    }

    /** Adds the four logical Knowledge Sync fields to row options for Metadata projection. */
    static void addKnowledgeSyncMetadata(
            SeaTunnelRow row,
            String sourceUri,
            String documentId,
            String documentHash,
            String chunkHash) {
        Map<String, Object> options = row.getOptions();
        options.put(KnowledgeSyncMetadataField.SOURCE_URI.getName(), sourceUri);
        options.put(KnowledgeSyncMetadataField.DOCUMENT_ID.getName(), documentId);
        options.put(KnowledgeSyncMetadataField.DOCUMENT_HASH.getName(), documentHash);
        options.put(KnowledgeSyncMetadataField.CHUNK_HASH.getName(), chunkHash);
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
