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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.apache.poi.xwpf.usermodel.IBodyElement;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFHyperlink;
import org.apache.poi.xwpf.usermodel.XWPFHyperlinkRun;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Objects;

/**
 * Reads Word (.docx, OOXML) documents for RAG/document-ingestion scenarios, emitting one row per
 * paragraph or table found in document order. Only ".docx" is supported: the legacy binary ".doc"
 * format is a different container that Apache POI reads through a separate API and is out of scope
 * here. Formatting metadata (font, color, alignment, hyperlinks) is aggregated at the paragraph
 * level because Apache POI does not expose finer-grained position information.
 */
@Slf4j
public class WordReadStrategy extends AbstractReadStrategy {

    private static final String DEFAULT_TEXT_COLOR = "000000";

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        Path tempDocxPath = null;
        try {
            tempDocxPath = createTempDocxPath();
            try (InputStream inputStream = hadoopFileSystemProxy.getInputStream(path)) {
                Files.copy(inputStream, tempDocxPath, StandardCopyOption.REPLACE_EXISTING);
            }

            try (XWPFDocument document = new XWPFDocument(Files.newInputStream(tempDocxPath))) {
                int elementId = 1;
                processDocumentInOrder(document, output, tableId, elementId);
            }
        } catch (Exception e) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_READ_FAILED,
                    "Failed to read Word document: " + path,
                    e);
        } finally {
            if (tempDocxPath != null) {
                deleteTempDocxPath(tempDocxPath);
            }
        }
    }

    Path createTempDocxPath() throws IOException {
        return Files.createTempFile("seatunnel-word-read-", ".docx");
    }

    void deleteTempDocxPath(Path tempDocxPath) throws IOException {
        Files.deleteIfExists(tempDocxPath);
    }

    private void processDocumentInOrder(
            XWPFDocument document, Collector<SeaTunnelRow> output, String tableId, int elementId) {

        for (IBodyElement element : document.getBodyElements()) {
            if (element instanceof XWPFParagraph) {
                XWPFParagraph paragraph = (XWPFParagraph) element;
                if (paragraph.getText() != null && !paragraph.getText().trim().isEmpty()) {
                    SeaTunnelRow row = createParagraphRow(paragraph, elementId++, tableId);
                    output.collect(row);
                }
            } else if (element instanceof XWPFTable) {
                XWPFTable table = (XWPFTable) element;
                String tableData = extractTableData(table);
                if (!tableData.trim().isEmpty()) {
                    SeaTunnelRow row = createTableRow(tableData, elementId++, tableId);
                    output.collect(row);
                }
            }
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        return new SeaTunnelRowType(
                new String[] {
                    "element_id",
                    "element_type",
                    "text",
                    "font_style",
                    "underline_style",
                    "font_size",
                    "font_family",
                    "text_color",
                    "alignment",
                    "hyperlink_url"
                },
                new SeaTunnelDataType[] {
                    BasicType.INT_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE
                });
    }

    private SeaTunnelRow createParagraphRow(
            XWPFParagraph paragraph, int elementId, String tableId) {
        Object[] fields = new Object[10];

        fields[0] = elementId;
        fields[1] = "paragraph";
        fields[2] = paragraph.getText();

        fields[3] = getFontStyle(paragraph);
        fields[4] = getUnderlineStyle(paragraph);
        fields[5] = getFontSize(paragraph);
        fields[6] = getFontFamily(paragraph);
        fields[7] = getTextColor(paragraph);

        fields[8] = paragraph.getAlignment().name();

        fields[9] = getHyperlinkUrl(paragraph);

        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setTableId(tableId);
        return row;
    }

    private SeaTunnelRow createTableRow(String tableData, int elementId, String tableId) {
        Object[] fields = new Object[10];

        fields[0] = elementId;
        fields[1] = "table";
        fields[2] = tableData;

        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setTableId(tableId);
        return row;
    }

    private String getFontStyle(XWPFParagraph paragraph) {
        boolean isBold = paragraph.getRuns().stream().anyMatch(XWPFRun::isBold);
        boolean isItalic = paragraph.getRuns().stream().anyMatch(XWPFRun::isItalic);

        if (isBold && isItalic) {
            return "BOLD_ITALIC";
        } else if (isBold) {
            return "BOLD";
        } else if (isItalic) {
            return "ITALIC";
        } else {
            return "NORMAL";
        }
    }

    private String getUnderlineStyle(XWPFParagraph paragraph) {
        return paragraph.getRuns().stream()
                .map(
                        run -> {
                            if (run.getUnderline() == null) {
                                return null;
                            }
                            return run.getUnderline().toString();
                        })
                .filter(style -> !"NONE".equals(style))
                .findFirst()
                .orElse(null);
    }

    private Integer getFontSize(XWPFParagraph paragraph) {
        return paragraph.getRuns().stream()
                .map(XWPFRun::getFontSize)
                .filter(size -> size > 0)
                .findFirst()
                .orElse(null);
    }

    private String getFontFamily(XWPFParagraph paragraph) {
        return paragraph.getRuns().stream()
                .map(XWPFRun::getFontFamily)
                .filter(font -> font != null && !font.isEmpty())
                .findFirst()
                .orElse(null);
    }

    private String getHyperlinkUrl(XWPFParagraph paragraph) {
        List<String> urls =
                paragraph.getRuns().stream()
                        .filter(XWPFHyperlinkRun.class::isInstance)
                        .map(run -> ((XWPFHyperlinkRun) run).getHyperlink(paragraph.getDocument()))
                        .filter(Objects::nonNull)
                        .map(XWPFHyperlink::getURL)
                        .filter(url -> url != null && !url.isEmpty())
                        .collect(java.util.stream.Collectors.toList());

        if (urls.isEmpty()) {
            return null;
        } else if (urls.size() == 1) {
            return urls.get(0);
        } else {
            return String.join(",", urls);
        }
    }

    private String getTextColor(XWPFParagraph paragraph) {
        return paragraph.getRuns().stream()
                .map(XWPFRun::getColor)
                .filter(color -> color != null && !color.isEmpty())
                .findFirst()
                .orElse(DEFAULT_TEXT_COLOR);
    }

    private String extractTableData(XWPFTable table) {
        StringBuilder tableData = new StringBuilder();
        List<XWPFTableRow> rows = table.getRows();

        for (XWPFTableRow row : rows) {
            List<XWPFTableCell> cells = row.getTableCells();
            StringBuilder rowData = new StringBuilder();

            for (int i = 0; i < cells.size(); i++) {
                XWPFTableCell cell = cells.get(i);
                String cellText = cell.getText();
                if (cellText != null) {
                    rowData.append(cellText.trim());
                }
                if (i < cells.size() - 1) {
                    rowData.append(" | ");
                }
            }

            if (rowData.length() > 0) {
                tableData.append(rowData);
                if (row != rows.get(rows.size() - 1)) {
                    tableData.append("\n");
                }
            }
        }

        return tableData.toString();
    }

    @Override
    public void setCatalogTable(CatalogTable catalogTable) {}

    @Override
    public void setPluginConfig(
            org.apache.seatunnel.shade.com.typesafe.config.Config pluginConfig) {}

    @Override
    public SeaTunnelRowType getActualSeaTunnelRowTypeInfo() {
        return new SeaTunnelRowType(
                new String[] {
                    "element_id",
                    "element_type",
                    "text",
                    "font_style",
                    "underline_style",
                    "font_size",
                    "font_family",
                    "text_color",
                    "alignment",
                    "hyperlink_url"
                },
                new SeaTunnelDataType[] {
                    BasicType.INT_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE
                });
    }
}
