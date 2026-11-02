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
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.DocumentElement;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.FileSourceDocumentRouting;
import org.apache.seatunnel.connectors.seatunnel.file.source.MarkdownKnowledgeSyncMetadata;

import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.model.StyleDescription;
import org.apache.poi.hwpf.usermodel.CharacterRun;
import org.apache.poi.hwpf.usermodel.Paragraph;
import org.apache.poi.hwpf.usermodel.Range;
import org.apache.poi.hwpf.usermodel.Table;
import org.apache.poi.hwpf.usermodel.TableCell;
import org.apache.poi.hwpf.usermodel.TableRow;
import org.apache.poi.xwpf.usermodel.IBodyElement;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Reads legacy Word and OOXML documents as ordered rows for document ingestion. */
@Slf4j
public class WordReadStrategy extends AbstractReadStrategy {

    private static final String[] FIELD_NAMES = {
        "element_id",
        "element_type",
        "heading_level",
        "text",
        "page_number",
        "position_index",
        "parent_id",
        "child_ids"
    };
    private static final SeaTunnelDataType<?>[] FIELD_TYPES = {
        BasicType.STRING_TYPE,
        BasicType.STRING_TYPE,
        BasicType.INT_TYPE,
        BasicType.STRING_TYPE,
        BasicType.INT_TYPE,
        BasicType.INT_TYPE,
        BasicType.STRING_TYPE,
        ArrayType.STRING_ARRAY_TYPE,
    };
    private static final String[] RAG_FIELD_NAMES = {
        "source_uri", "document_id", "chunk_id", "chunk_index", "content_hash"
    };
    private static final SeaTunnelDataType<?>[] RAG_FIELD_TYPES = {
        BasicType.STRING_TYPE,
        BasicType.STRING_TYPE,
        BasicType.STRING_TYPE,
        BasicType.INT_TYPE,
        BasicType.STRING_TYPE
    };
    private static final Pattern HEADING_STYLE = Pattern.compile("(?i)^(?:heading|标题)\\s*([1-9])$");

    private boolean ragMetadataEnabled =
            FileBaseSourceOptions.WORD_RAG_METADATA_ENABLED.defaultValue();

    private static final class WordElement extends DocumentElement {
        private WordElement(String type, Integer headingLevel, String text) {
            super(type, text);
            setHeadingLevel(headingLevel);
        }
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        String extension = getWordExtension(path);
        if (extension == null) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.FORMAT_NOT_SUPPORT,
                    "Word reader supports only .doc and .docx files: "
                            + MarkdownKnowledgeSyncMetadata.safeSourceContext(path));
        }

        Path stagedFile = null;
        try {
            stagedFile = Files.createTempFile("seatunnel-word-read-", extension);
            MessageDigest documentDigest = MarkdownKnowledgeSyncMetadata.newSha256Digest();
            try (InputStream in = hadoopFileSystemProxy.getInputStream(path);
                    DigestInputStream digestIn = new DigestInputStream(in, documentDigest);
                    OutputStream out = Files.newOutputStream(stagedFile)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = digestIn.read(buffer)) != -1) {
                    out.write(buffer, 0, read);
                }
            }

            List<WordElement> elements =
                    ".docx".equals(extension) ? readDocx(stagedFile) : readDoc(stagedFile);
            assignHierarchy(elements);
            String sourceUri =
                    ragMetadataEnabled
                            ? MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(path)
                            : null;
            String documentId =
                    ragMetadataEnabled
                            ? MarkdownKnowledgeSyncMetadata.buildDocumentId(sourceUri)
                            : null;
            String documentHash = MarkdownKnowledgeSyncMetadata.toLowerHex(documentDigest.digest());
            int chunkIndex = 0;
            for (int i = 0; i < elements.size(); i++) {
                WordElement element = elements.get(i);
                element.setPositionIndex(i + 1);
                if (ragMetadataEnabled) {
                    chunkIndex++;
                }
                SeaTunnelRow row =
                        toRow(
                                element,
                                tableId,
                                ragMetadataEnabled ? sourceUri : null,
                                ragMetadataEnabled ? documentId : null,
                                ragMetadataEnabled ? documentHash : null,
                                chunkIndex);
                output.collect(row);
            }
        } catch (FileConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_READ_FAILED,
                    "Failed to read Word document: "
                            + MarkdownKnowledgeSyncMetadata.safeSourceContext(path),
                    e);
        } finally {
            if (stagedFile != null) {
                try {
                    Files.deleteIfExists(stagedFile);
                } catch (IOException e) {
                    log.warn("Failed to delete temporary Word file [{}]", stagedFile, e);
                }
            }
        }
    }

    private List<WordElement> readDocx(Path path) throws IOException {
        List<WordElement> elements = new ArrayList<>();
        try (InputStream in = Files.newInputStream(path);
                XWPFDocument document = new XWPFDocument(in)) {
            for (IBodyElement bodyElement : document.getBodyElements()) {
                if (bodyElement instanceof XWPFParagraph) {
                    XWPFParagraph paragraph = (XWPFParagraph) bodyElement;
                    String text = normalizeParagraphText(paragraph.getText());
                    if (!text.isEmpty()) {
                        Integer headingLevel = getHeadingLevel(paragraph);
                        elements.add(
                                new WordElement(
                                        headingLevel == null ? "paragraph" : "heading",
                                        headingLevel,
                                        text));
                    }
                } else if (bodyElement instanceof XWPFTable) {
                    String text = getTableText((XWPFTable) bodyElement);
                    if (!text.isEmpty()) {
                        elements.add(new WordElement("table", null, text));
                    }
                }
            }
        }
        return elements;
    }

    private List<WordElement> readDoc(Path path) throws IOException {
        List<WordElement> elements = new ArrayList<>();
        try (InputStream in = Files.newInputStream(path);
                HWPFDocument document = new HWPFDocument(in)) {
            Range range = document.getRange();
            int paragraphIndex = 0;
            int consumedUntil = -1;
            while (paragraphIndex < range.numParagraphs()) {
                Paragraph paragraph = range.getParagraph(paragraphIndex);
                if (paragraph.getStartOffset() < consumedUntil) {
                    paragraphIndex++;
                    continue;
                }
                if (paragraph.isInTable()) {
                    Table table = range.getTable(paragraph);
                    String text = getTableText(table);
                    if (!text.isEmpty()) {
                        elements.add(new WordElement("table", null, text));
                    }
                    consumedUntil = table.getEndOffset();
                    paragraphIndex++;
                    continue;
                }

                String text = normalizeParagraphText(paragraph.text());
                if (!text.isEmpty()) {
                    Integer headingLevel = getHeadingLevel(document, paragraph);
                    elements.add(
                            new WordElement(
                                    headingLevel == null ? "paragraph" : "heading",
                                    headingLevel,
                                    text));
                }
                paragraphIndex++;
            }
        }
        return elements;
    }

    private Integer getHeadingLevel(XWPFParagraph paragraph) {
        String styleId = paragraph.getStyleID();
        if (styleId != null) {
            Matcher matcher = HEADING_STYLE.matcher(styleId.replace("_", " "));
            if (matcher.matches()) {
                return Integer.valueOf(matcher.group(1));
            }
        }
        if (paragraph.getCTP().getPPr() != null
                && paragraph.getCTP().getPPr().getOutlineLvl() != null) {
            return paragraph.getCTP().getPPr().getOutlineLvl().getVal().intValue() + 1;
        }
        return null;
    }

    private Integer getHeadingLevel(HWPFDocument document, Paragraph paragraph) {
        // HWPF exposes the stored outline level as zero-based; 9 means no outline level.
        int outlineLevel = paragraph.getLvl();
        if (outlineLevel >= 0 && outlineLevel < 9) {
            return outlineLevel + 1;
        }

        StyleDescription style =
                document.getStyleSheet().getStyleDescription(paragraph.getStyleIndex());
        if (style != null && style.getName() != null) {
            Matcher matcher = HEADING_STYLE.matcher(style.getName().trim());
            if (matcher.matches()) {
                return Integer.valueOf(matcher.group(1));
            }
        }

        return getHeadingLevelFromCharacterFormatting(document, paragraph);
    }

    private Integer getHeadingLevelFromCharacterFormatting(
            HWPFDocument document, Paragraph paragraph) {
        List<CharacterRun> textRuns = new ArrayList<>();
        for (int i = 0; i < paragraph.numCharacterRuns(); i++) {
            CharacterRun run = paragraph.getCharacterRun(i);
            if (!run.text().trim().isEmpty()) {
                textRuns.add(run);
            }
        }
        if (textRuns.isEmpty()) {
            return null;
        }

        Integer matchedLevel = null;
        for (int i = 0; i < document.getStyleSheet().numStyles(); i++) {
            StyleDescription style = document.getStyleSheet().getStyleDescription(i);
            if (style == null || style.getName() == null || style.getCHP() == null) {
                continue;
            }
            Matcher matcher = HEADING_STYLE.matcher(style.getName().trim());
            if (!matcher.matches()) {
                continue;
            }

            boolean matchesStyle = true;
            for (CharacterRun run : textRuns) {
                if (run.getFontSize() != style.getCHP().getFontSize()
                        || run.isBold() != style.getCHP().isBold()) {
                    matchesStyle = false;
                    break;
                }
            }
            if (!matchesStyle) {
                continue;
            }

            int level = Integer.parseInt(matcher.group(1));
            if (matchedLevel != null && matchedLevel != level) {
                return null;
            }
            matchedLevel = level;
        }
        return matchedLevel;
    }

    private void assignHierarchy(List<WordElement> elements) {
        List<WordElement> headingStack = new ArrayList<>();
        for (WordElement element : elements) {
            if (element.getHeadingLevel() != null) {
                while (!headingStack.isEmpty()
                        && headingStack.get(headingStack.size() - 1).getHeadingLevel()
                                >= element.getHeadingLevel()) {
                    headingStack.remove(headingStack.size() - 1);
                }
                if (!headingStack.isEmpty()) {
                    WordElement parent = headingStack.get(headingStack.size() - 1);
                    element.setParentId(parent.getElementId());
                    parent.addChildId(element.getElementId());
                }
                headingStack.add(element);
            } else if (!headingStack.isEmpty()) {
                WordElement parent = headingStack.get(headingStack.size() - 1);
                element.setParentId(parent.getElementId());
                parent.addChildId(element.getElementId());
            }
        }
    }

    private SeaTunnelRow toRow(
            WordElement element,
            String tableId,
            String sourceUri,
            String documentId,
            String documentHash,
            int chunkIndex) {
        Object[] fields = {
            element.getElementId(),
            element.getElementType(),
            element.getHeadingLevel(),
            element.getText(),
            element.getPageNumber(),
            element.getPositionIndex(),
            element.getParentId(),
            element.getChildIds().toArray(new String[0])
        };
        Object[] rowFields = fields;
        if (ragMetadataEnabled) {
            String contentHash = FileSourceDocumentRouting.sha256Hex(element.getText());
            String chunkId =
                    "chunk_"
                            + FileSourceDocumentRouting.sha256Hex(
                                    documentId + ":" + chunkIndex + ":" + contentHash);
            rowFields = new Object[fields.length + RAG_FIELD_NAMES.length];
            System.arraycopy(fields, 0, rowFields, 0, fields.length);
            rowFields[fields.length] = sourceUri;
            rowFields[fields.length + 1] = documentId;
            rowFields[fields.length + 2] = chunkId;
            rowFields[fields.length + 3] = chunkIndex;
            rowFields[fields.length + 4] = contentHash;
        }
        SeaTunnelRow row = new SeaTunnelRow(rowFields);
        row.setTableId(tableId);
        if (ragMetadataEnabled) {
            MarkdownReadStrategy.addKnowledgeSyncMetadata(
                    row,
                    sourceUri,
                    documentId,
                    documentHash,
                    FileSourceDocumentRouting.sha256Hex(element.getText()));
        }
        return row;
    }

    private String getTableText(XWPFTable table) {
        List<String> rows = new ArrayList<>();
        for (XWPFTableRow row : table.getRows()) {
            List<String> cells = new ArrayList<>();
            for (XWPFTableCell cell : row.getTableCells()) {
                cells.add(normalizeParagraphText(cell.getText()));
            }
            rows.add(String.join(" | ", cells));
        }
        return String.join("\n", rows).trim();
    }

    private String getTableText(Table table) {
        List<String> rows = new ArrayList<>();
        for (int rowIndex = 0; rowIndex < table.numRows(); rowIndex++) {
            TableRow row = table.getRow(rowIndex);
            List<String> cells = new ArrayList<>();
            for (int cellIndex = 0; cellIndex < row.numCells(); cellIndex++) {
                TableCell cell = row.getCell(cellIndex);
                cells.add(normalizeParagraphText(cell.text()));
            }
            rows.add(String.join(" | ", cells));
        }
        return String.join("\n", rows).trim();
    }

    /**
     * Cleans text returned by POI by removing Word table markers and normalizing whitespace and
     * line endings.
     */
    private String normalizeParagraphText(String text) {
        if (text == null) {
            return "";
        }
        return text.replace('\u0007', ' ').replace('\r', '\n').replaceAll("[\\t ]+", " ").trim();
    }

    private String getWordExtension(String path) {
        String normalized = path.toLowerCase(Locale.ROOT);
        int query = normalized.indexOf('?');
        if (query >= 0) {
            normalized = normalized.substring(0, query);
        }
        if (normalized.endsWith(".docx")) {
            return ".docx";
        }
        return normalized.endsWith(".doc") ? ".doc" : null;
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        return ragMetadataEnabled
                ? new SeaTunnelRowType(
                        concat(FIELD_NAMES, RAG_FIELD_NAMES), concat(FIELD_TYPES, RAG_FIELD_TYPES))
                : new SeaTunnelRowType(FIELD_NAMES, FIELD_TYPES);
    }

    @Override
    public void setPluginConfig(Config pluginConfig) {
        super.setPluginConfig(pluginConfig);
        if (pluginConfig.hasPath(FileBaseSourceOptions.WORD_RAG_METADATA_ENABLED.key())) {
            ragMetadataEnabled =
                    pluginConfig.getBoolean(FileBaseSourceOptions.WORD_RAG_METADATA_ENABLED.key());
        }
    }

    @Override
    public SeaTunnelRowType getActualSeaTunnelRowTypeInfo() {
        return getSeaTunnelRowTypeInfo(null);
    }

    private static String[] concat(String[] left, String[] right) {
        String[] result = new String[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    private static SeaTunnelDataType<?>[] concat(
            SeaTunnelDataType<?>[] left, SeaTunnelDataType<?>[] right) {
        SeaTunnelDataType<?>[] result = new SeaTunnelDataType<?>[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }
}
