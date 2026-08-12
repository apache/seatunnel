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
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

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

import org.apache.pdfbox.Loader;
import org.apache.pdfbox.contentstream.PDFStreamEngine;
import org.apache.pdfbox.contentstream.operator.Operator;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentCatalog;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.graphics.PDXObject;
import org.apache.pdfbox.pdmodel.graphics.form.PDFormXObject;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.apache.pdfbox.pdmodel.interactive.action.PDAction;
import org.apache.pdfbox.pdmodel.interactive.action.PDActionGoTo;
import org.apache.pdfbox.pdmodel.interactive.action.PDActionURI;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotation;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotationLink;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.destination.PDDestination;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.destination.PDNamedDestination;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.destination.PDPageDestination;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.destination.PDPageXYZDestination;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDDocumentOutline;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDOutlineItem;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;
import org.apache.pdfbox.text.TextPosition;
import org.apache.pdfbox.util.Matrix;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * PDF reading strategy V2 for RAG scenarios Extracts structured content including chapter titles,
 * paragraph text, and images
 */
@Slf4j
public class PdfReadStrategy extends AbstractReadStrategy {

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
        ArrayType.STRING_ARRAY_TYPE
    };
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

    private boolean pdfRagMetadataEnabled =
            FileBaseSourceOptions.PDF_RAG_METADATA_ENABLED.defaultValue();

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {

        Path tempPdfPath = null;
        try {
            tempPdfPath = createTempPdfPath();
            try (InputStream inputStream = hadoopFileSystemProxy.getInputStream(path)) {
                Files.copy(inputStream, tempPdfPath, StandardCopyOption.REPLACE_EXISTING);
            }

            try (PDDocument document = Loader.loadPDF(tempPdfPath.toFile())) {
                List<DocumentElement> elements = extractPdfDocumentElements(document);
                String sourceUri = normalizeSourceUri(path);
                String documentId = buildDocumentId(sourceUri);

                log.info(
                        "PDF file '{}' processed successfully, generated {} elements",
                        path,
                        elements.size());

                int chunkIndex = 1;
                for (DocumentElement element : elements) {
                    output.collect(
                            toSeaTunnelRow(
                                    element,
                                    sourceUri,
                                    documentId,
                                    chunkIndex++,
                                    pdfRagMetadataEnabled));
                }
            }
        } catch (Exception e) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_READ_FAILED,
                    String.format("Failed to parse PDF document [%s]: %s", path, e.getMessage()),
                    e);
        } finally {
            if (tempPdfPath != null) {
                deleteTempPdfPath(tempPdfPath);
            }
        }
    }

    Path createTempPdfPath() throws IOException {
        return Files.createTempFile("seatunnel-pdf-read-", ".pdf");
    }

    void deleteTempPdfPath(Path tempPdfPath) throws IOException {
        Files.deleteIfExists(tempPdfPath);
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        if (pdfRagMetadataEnabled) {
            return new SeaTunnelRowType(
                    concat(DEFAULT_FIELD_NAMES, RAG_METADATA_FIELD_NAMES),
                    concat(DEFAULT_FIELD_TYPES, RAG_METADATA_FIELD_TYPES));
        }
        return new SeaTunnelRowType(DEFAULT_FIELD_NAMES, DEFAULT_FIELD_TYPES);
    }

    @Override
    public void setPluginConfig(Config pluginConfig) {
        super.setPluginConfig(pluginConfig);
        if (pluginConfig.hasPath(FileBaseSourceOptions.PDF_RAG_METADATA_ENABLED.key())) {
            pdfRagMetadataEnabled =
                    pluginConfig.getBoolean(FileBaseSourceOptions.PDF_RAG_METADATA_ENABLED.key());
        }
    }

    /** Extract all document elements including headings, paragraphs, and images */
    private List<DocumentElement> extractPdfDocumentElements(PDDocument document)
            throws IOException {
        List<DocumentElement> elements = new ArrayList<>();

        PDDocumentCatalog catalog = document.getDocumentCatalog();
        PDDocumentOutline documentOutline = catalog.getDocumentOutline();

        // Check if outline exists
        if (documentOutline != null && documentOutline.getFirstChild() != null) {
            List<CoordinateInfo> headingCoordinates = new ArrayList<>();

            // Extract headings from outline
            List<DocumentElement> headings =
                    extractHeadingsFromOutline(document, documentOutline, headingCoordinates);

            // Build coordinate lookup map for O(1) access
            Map<String, CoordinateInfo> coordMap =
                    headingCoordinates.stream()
                            .filter(c -> c.getElementId() != null)
                            .collect(
                                    Collectors.toMap(
                                            CoordinateInfo::getElementId, c -> c, (a, b) -> a));

            // Extract paragraphs and associate with headings
            List<DocumentElement> paragraphs =
                    extractParagraphsForHeadings(document, headings, coordMap);

            // Extract images and associate with headings
            List<DocumentElement> images = extractImages(document, headingCoordinates);

            List<DocumentElement> links = extractLinks(document, headingCoordinates);

            // Merge headings and paragraphs maintaining hierarchy
            elements.addAll(mergeElements(headings, paragraphs, images, links));
        } else {
            // No outline available, treat all content as paragraphs
            log.info("No outline found in PDF, processing only paragraphs and images elements");
            elements.addAll(extractParagraphsAndImages(document));
        }

        return elements;
    }

    /** Extract headings from PDF outline structure with coordinate information */
    private List<DocumentElement> extractHeadingsFromOutline(
            PDDocument document, PDDocumentOutline outline, List<CoordinateInfo> headingCoordinates)
            throws IOException {
        List<DocumentElement> headings = new ArrayList<>();

        PDOutlineItem current = outline.getFirstChild();
        int positionIndex = 0;

        while (current != null) {
            positionIndex =
                    extractOutlineItemRecursively(
                            document,
                            current,
                            null,
                            1,
                            positionIndex,
                            headings,
                            headingCoordinates);
            current = current.getNextSibling();
        }

        log.debug("Extracted {} headings from outline with coordinates", headings.size());
        return headings;
    }

    /** Recursively extract outline items and build heading hierarchy with coordinates */
    private int extractOutlineItemRecursively(
            PDDocument document,
            PDOutlineItem item,
            String parentId,
            int level,
            int positionIndex,
            List<DocumentElement> headings,
            List<CoordinateInfo> coordinates)
            throws IOException {

        DocumentElement heading =
                convertOutlineToHeading(document, item, parentId, level, positionIndex);
        if (heading == null) {
            return positionIndex;
        }

        headings.add(heading);
        positionIndex++;

        // Extract coordinate information
        CoordinateInfo coord = extractCoordinateFromOutline(document, item, heading.getElementId());
        if (coord != null) {
            coordinates.add(coord);
        }

        // Process children
        PDOutlineItem child = item.getFirstChild();
        while (child != null) {
            int startIndex = positionIndex;
            positionIndex =
                    extractOutlineItemRecursively(
                            document,
                            child,
                            heading.getElementId(),
                            level + 1,
                            positionIndex,
                            headings,
                            coordinates);

            // Update parent's child_ids with direct children only
            if (positionIndex > startIndex) {
                for (int i = startIndex; i < positionIndex && i < headings.size(); i++) {
                    DocumentElement childElement = headings.get(i);
                    if (heading.getElementId().equals(childElement.getParentId())) {
                        heading.addChildId(childElement.getElementId());
                        break;
                    }
                }
            }

            child = child.getNextSibling();
        }

        return positionIndex;
    }

    /** Convert PDF outline item to DocumentElement heading */
    private DocumentElement convertOutlineToHeading(
            PDDocument document, PDOutlineItem item, String parentId, int level, int positionIndex)
            throws IOException {

        if (item == null || StringUtils.isBlank(item.getTitle())) {
            return null;
        }

        String title = item.getTitle().trim();
        DocumentElement heading = new DocumentElement("heading", title);
        heading.setHeadingLevel(Math.min(level, 6)); // Limit to max level 6
        heading.setPositionIndex(positionIndex);
        heading.setParentId(parentId);

        // Get page number from destination
        try {
            PDPage destinationPage = item.findDestinationPage(document);
            if (destinationPage != null) {
                int pageIndex = document.getPages().indexOf(destinationPage);
                if (pageIndex >= 0) {
                    heading.setPageNumber(pageIndex + 1);
                }
            }
        } catch (IOException e) {
            log.warn("Could not determine page number for heading: {}", title);
        }

        return heading;
    }

    /** Extract coordinate information from PDF outline item */
    private CoordinateInfo extractCoordinateFromOutline(
            PDDocument document, PDOutlineItem item, String elementId) {
        try {
            PDDestination destination = item.getDestination();

            if (destination == null) {
                // Check for action-based destinations
                PDAction action = item.getAction();
                if (action instanceof PDActionGoTo) {
                    destination = ((PDActionGoTo) action).getDestination();
                }
            }

            PDPageDestination pageDestination = null;
            if (destination instanceof PDNamedDestination) {
                pageDestination =
                        document.getDocumentCatalog()
                                .findNamedDestinationPage((PDNamedDestination) destination);
                if (pageDestination == null) {
                    return null;
                }

            } else if (destination instanceof PDPageDestination) {
                pageDestination = (PDPageDestination) destination;
            }

            if (pageDestination instanceof PDPageXYZDestination) {
                PDPageXYZDestination xyzDest = (PDPageXYZDestination) pageDestination;

                // Get page number
                int pageNumber = xyzDest.retrievePageNumber();
                if (pageNumber < 0) {
                    PDPage page = xyzDest.getPage();
                    if (page != null) {
                        pageNumber = document.getPages().indexOf(page);
                    }
                }

                // Get coordinates - note that PDF coordinates have origin at bottom-left
                float x = xyzDest.getLeft();
                float y = xyzDest.getTop();

                return new CoordinateInfo(pageNumber, x, y, elementId);
            }

        } catch (IOException e) {
            log.warn("Could not extract coordinates for outline item: {}", item.getTitle());
        }

        return null;
    }

    /** Extract paragraphs for each heading using coordinate-based approach */
    private List<DocumentElement> extractParagraphsForHeadings(
            PDDocument document,
            List<DocumentElement> headings,
            Map<String, CoordinateInfo> coordMap)
            throws IOException {

        List<DocumentElement> paragraphs = new ArrayList<>();

        for (int i = 0; i < headings.size(); i++) {
            DocumentElement heading = headings.get(i);
            DocumentElement nextHeading = (i + 1 < headings.size()) ? headings.get(i + 1) : null;

            String paragraphText =
                    extractParagraphUsingCoordinates(document, heading, nextHeading, coordMap);

            paragraphText = cleanParagraphText(paragraphText);

            if (StringUtils.isNotBlank(paragraphText)) {
                DocumentElement paragraph = new DocumentElement("paragraph", paragraphText.trim());
                paragraph.setParentId(heading.getElementId());
                paragraph.setPageNumber(heading.getPageNumber());
                paragraph.setHeadingLevel(null);
                paragraph.setChildIds(new ArrayList<>());
                paragraphs.add(paragraph);
            }
        }

        return paragraphs;
    }

    /** Extract paragraph text using coordinate information */
    private String extractParagraphUsingCoordinates(
            PDDocument document,
            DocumentElement heading,
            DocumentElement nextHeading,
            Map<String, CoordinateInfo> coordMap)
            throws IOException {

        CoordinateInfo currentCoord = coordMap.get(heading.getElementId());
        CoordinateInfo nextCoord =
                (nextHeading != null) ? coordMap.get(nextHeading.getElementId()) : null;

        if (currentCoord != null) {
            return extractTextUsingCoordinates(document, currentCoord, nextCoord);
        } else {
            log.info(
                    "No coordinates found for heading: {}, falling back to text-based extraction",
                    heading.getText());
            List<String> pageContents = extractPageContents(document);
            return extractParagraphTextForHeading(heading, nextHeading, pageContents);
        }
    }

    /** Extract text using coordinate information */
    private String extractTextUsingCoordinates(
            PDDocument document, CoordinateInfo startCoord, CoordinateInfo endCoord)
            throws IOException {

        int startPageIndex = startCoord.getPageNumber();
        int endPageIndex =
                endCoord != null ? endCoord.getPageNumber() : document.getNumberOfPages() - 1;

        float startCoordY = startCoord.getY();
        float endCoordY = endCoord == null ? 0 : endCoord.getY();

        // Obtain the Y-axis coordinate at the beginning of the paragraph
        ParagraphCoordinateExtractor paragraphExtractor =
                new ParagraphCoordinateExtractor(document);
        float paragraphEndY = paragraphExtractor.getParagraphEndY(startCoord, startPageIndex + 1);

        if (paragraphEndY != 0.0f) {
            startCoordY = paragraphEndY;
        }

        PDFTextStripperByArea stripper = new PDFTextStripperByArea();
        stripper.setSortByPosition(true);

        StringBuilder content = new StringBuilder();

        float regionStartY, regionHeight;

        for (int pageIndex = startPageIndex; pageIndex <= endPageIndex; pageIndex++) {
            PDPage page = document.getPage(pageIndex);
            PDRectangle mediaBox = page.getMediaBox();

            if (pageIndex == startPageIndex && pageIndex == endPageIndex) {
                regionStartY = mediaBox.getHeight() - startCoordY;
                regionHeight = startCoordY - endCoordY;
            } else {
                if (pageIndex == startPageIndex) {
                    regionStartY = mediaBox.getHeight() - startCoordY;
                    regionHeight = startCoordY;
                } else if (pageIndex == endPageIndex) {
                    regionStartY = 0;
                    regionHeight = mediaBox.getHeight() - endCoordY;
                } else {
                    regionStartY = 0;
                    regionHeight = mediaBox.getHeight();
                }
            }

            // Create an extraction area
            Rectangle2D region =
                    new Rectangle2D.Float(0, regionStartY, mediaBox.getWidth(), regionHeight);

            stripper.addRegion("page" + pageIndex, region);
            stripper.extractRegions(page);

            String pageContent = stripper.getTextForRegion("page" + pageIndex);
            if (pageContent != null && !pageContent.trim().isEmpty()) {
                content.append(pageContent).append("\n");
            }
        }

        return content.toString().trim();
    }

    /** Extract page contents as text */
    private List<String> extractPageContents(PDDocument document) throws IOException {
        List<String> pageContents = new ArrayList<>();
        PDFTextStripper stripper = new PDFTextStripper();
        stripper.setSortByPosition(true);

        for (int i = 1; i <= document.getNumberOfPages(); i++) {
            stripper.setStartPage(i);
            stripper.setEndPage(i);
            String pageText = stripper.getText(document);

            // Remove header/footer noise
            pageContents.add(pageText);
        }

        return pageContents;
    }

    /** Extract paragraph text for a specific heading */
    private String extractParagraphTextForHeading(
            DocumentElement heading, DocumentElement nextHeading, List<String> pageContents) {

        if (heading.getPageNumber() == null || heading.getPageNumber() < 1) {
            return "";
        }

        String headingTitle = heading.getText();
        String nextTitle = (nextHeading != null) ? nextHeading.getText() : null;
        Integer nextPageNum = (nextHeading != null) ? nextHeading.getPageNumber() : null;

        return extractTextBetweenHeadings(
                heading.getPageNumber(), nextPageNum, headingTitle, nextTitle, pageContents);
    }

    /** This method finds the actual content between the current heading and the next heading */
    private String extractTextBetweenHeadings(
            int startPage,
            Integer endPage,
            String startTitle,
            String endTitle,
            List<String> pageContents) {

        StringBuilder content = new StringBuilder();
        boolean startCollecting = false;
        boolean foundStartTitle = false;

        int maxPage = (endPage != null) ? endPage : pageContents.size();

        // Iterate through pages to find content between headings
        for (int page = startPage; page <= maxPage; page++) {
            String pageText = pageContents.get(page - 1);

            if (StringUtils.isBlank(pageText)) {
                continue;
            }

            String[] lines = pageText.split("\\n");

            for (String line : lines) {
                String trimmedLine = line.trim();

                if (StringUtils.isBlank(trimmedLine)) {
                    continue;
                }

                // Check if this line contains the starting heading
                if (!foundStartTitle
                        && StringUtils.isNotBlank(startTitle)
                        && lineContainsHeading(trimmedLine, startTitle)) {

                    // Skip the heading line itself
                    foundStartTitle = true;
                    startCollecting = true;
                    continue;
                }

                // Check if this line contains the ending heading
                if (foundStartTitle
                        && StringUtils.isNotBlank(endTitle)
                        && lineContainsHeading(trimmedLine, endTitle)) {

                    // Stop collecting when we hit the next heading
                    startCollecting = false;
                    break;
                }

                // Collect content between headings
                if (startCollecting) {
                    content.append(trimmedLine).append("\n");
                }
            }
        }

        return content.toString().trim();
    }

    /** Check if a line contains a specific heading */
    private boolean lineContainsHeading(String line, String heading) {
        if (StringUtils.isBlank(line) || StringUtils.isBlank(heading)) {
            return false;
        }

        if (line.equals(heading)) {
            return true;
        }

        // Check if line ends with the heading.
        // The reason for using the endsWith method is that
        // for the original title: "1.1 Introduction", the extract head title will only take the
        // value of "Introduction"
        if (line.endsWith(heading)) {
            if (line.length() == heading.length()) {
                return true;
            }
            char charBefore = line.charAt(line.length() - heading.length() - 1);
            return Character.isWhitespace(charBefore)
                    || Character.isDigit(charBefore)
                    || charBefore == '.'
                    || charBefore == ')';
        }

        return false;
    }

    /** Process document with no outline - only contains paragraphs and images element. */
    private List<DocumentElement> extractParagraphsAndImages(PDDocument document)
            throws IOException {
        List<DocumentElement> elements = new ArrayList<>();
        List<String> pageContents = extractPageContents(document);
        List<DocumentElement> images = extractImages(document, new ArrayList<>());

        for (int i = 0; i < pageContents.size(); i++) {
            String pageText = pageContents.get(i);
            if (StringUtils.isNotBlank(pageText)) {
                DocumentElement paragraph = new DocumentElement("paragraph", pageText.trim());
                paragraph.setPageNumber(i + 1);
                paragraph.setPositionIndex(i);
                elements.add(paragraph);
            }
        }

        elements.addAll(images);

        // sorted by pageNumber
        elements.sort(Comparator.comparingInt(DocumentElement::getPageNumber));

        return elements;
    }

    List<DocumentElement> extractImages(
            PDDocument document, List<CoordinateInfo> headingCoordinates) throws IOException {

        PdfImageExtractor pdfImageExtractor = new PdfImageExtractor(document);

        for (int pageNum = 0; pageNum < document.getNumberOfPages(); pageNum++) {
            pdfImageExtractor.processPage(document.getPage(pageNum));
        }

        List<CoordinateInfo> imagesCoordinates = pdfImageExtractor.getImagesCoordinates();

        // When no headings exist (no-outline PDF), create flat image elements directly
        if (headingCoordinates.isEmpty()) {
            return createFlatImageElements(imagesCoordinates);
        }

        return assignElementsToHeadings(
                document,
                imagesCoordinates,
                headingCoordinates,
                "image",
                coord ->
                        String.format(
                                "image_page_%d_pos_(%.0f,%.0f)",
                                coord.getPageNumber() + 1, coord.getX(), coord.getY()));
    }

    /** Create flat image elements for PDFs without outline structure */
    private List<DocumentElement> createFlatImageElements(List<CoordinateInfo> imagesCoordinates) {
        List<DocumentElement> elements = new ArrayList<>();
        for (int i = 0; i < imagesCoordinates.size(); i++) {
            CoordinateInfo coord = imagesCoordinates.get(i);
            DocumentElement element = new DocumentElement();
            element.setElementType("image");
            element.setText(
                    String.format(
                            "image_page_%d_pos_(%.0f,%.0f)",
                            coord.getPageNumber() + 1, coord.getX(), coord.getY()));
            element.setPageNumber(coord.getPageNumber() + 1);
            element.setPositionIndex(i);
            element.setParentId(null);
            element.setChildIds(new ArrayList<>());
            elements.add(element);
        }
        return elements;
    }

    /** Determine if element belongs to a specific heading based on position rules */
    private boolean isElementBelongsToHead(
            CoordinateInfo elementCoordinate, CoordinateInfo currentHead, CoordinateInfo nextHead) {

        int elementPage = elementCoordinate.getPageNumber();
        float elementY = elementCoordinate.getY();

        int currentPage = currentHead.getPageNumber();
        float currentY = currentHead.getY();

        // Case 1: element is before the first heading
        if (elementPage < currentPage || (elementPage == currentPage && elementY > currentY)) {
            return false;
        }

        // Case 2: No next heading - element belongs to current heading if it's after current
        // heading
        if (nextHead == null) {
            return elementPage > currentPage || (elementPage == currentPage && elementY < currentY);
        }

        int nextPage = nextHead.getPageNumber();
        float nextY = nextHead.getY();

        // Case 3: element page is between current and next heading pages
        if (elementPage > currentPage && elementPage < nextPage) {
            return true;
        }

        // Case 4: element is on same page as current heading
        if (elementPage == currentPage) {
            // element must be below current heading
            if (elementY >= currentY) {
                return false;
            }

            // If next heading is on same page, element must be above next heading
            if (nextPage == currentPage) {
                return elementY > nextY;
            }

            // Next heading is on different page, element belongs to current heading
            return true;
        }

        // Case 5: element is on same page as next heading
        if (elementPage == nextPage) {
            // element must be above (higher Y) next heading to belong to current heading
            return elementY > nextY;
        }

        return false;
    }

    /** Obtain all the link elements in the document */
    public List<DocumentElement> extractLinks(
            PDDocument document, List<CoordinateInfo> headingCoordinates) throws IOException {

        List<CoordinateInfo> linksCoordinateInfo = new ArrayList<>();

        for (int pageNum = 0; pageNum < document.getNumberOfPages(); pageNum++) {
            PDPage page = document.getPage(pageNum);
            for (PDAnnotation annot : page.getAnnotations()) {
                if (annot instanceof PDAnnotationLink) {
                    PDAnnotationLink link = (PDAnnotationLink) annot;
                    PDRectangle rectangle = link.getRectangle();
                    PDAction action = link.getAction();
                    if (action instanceof PDActionURI) {
                        PDActionURI uri = (PDActionURI) action;
                        linksCoordinateInfo.add(
                                new CoordinateInfo(
                                        pageNum,
                                        rectangle.getUpperRightX(),
                                        rectangle.getUpperRightY(),
                                        null,
                                        uri.getURI()));
                    }
                }
            }
        }

        return assignElementsToHeadings(
                document, linksCoordinateInfo, headingCoordinates, "link", CoordinateInfo::getText);
    }

    private List<DocumentElement> assignElementsToHeadings(
            PDDocument document,
            List<CoordinateInfo> elementCoordinates,
            List<CoordinateInfo> headingCoordinates,
            String elementType,
            java.util.function.Function<CoordinateInfo, String> textExtractor) {

        List<DocumentElement> elements = new ArrayList<>();

        for (int i = 0; i < headingCoordinates.size(); i++) {
            CoordinateInfo heading = headingCoordinates.get(i);
            CoordinateInfo nextHeading =
                    (i + 1 < headingCoordinates.size()) ? headingCoordinates.get(i + 1) : null;
            int endPage =
                    nextHeading == null
                            ? document.getNumberOfPages()
                            : nextHeading.getPageNumber() + 1;

            IntStream.range(heading.getPageNumber(), endPage)
                    .forEach(
                            pageIndex ->
                                    elementCoordinates.stream()
                                            .filter(coord -> coord.getPageNumber() == pageIndex)
                                            .filter(
                                                    coord ->
                                                            isElementBelongsToHead(
                                                                    coord, heading, nextHeading))
                                            .forEach(
                                                    coord -> {
                                                        DocumentElement element =
                                                                new DocumentElement();
                                                        element.setParentId(heading.getElementId());
                                                        element.setPageNumber(
                                                                coord.getPageNumber() + 1);
                                                        element.setChildIds(new ArrayList<>());
                                                        element.setElementType(elementType);
                                                        String text = textExtractor.apply(coord);
                                                        if (text != null) {
                                                            element.setText(text);
                                                        }
                                                        elements.add(element);
                                                    }));
        }

        return elements;
    }

    /** Merge headings, paragraphs and images element */
    private List<DocumentElement> mergeElements(
            List<DocumentElement> headings,
            List<DocumentElement> paragraphs,
            List<DocumentElement> images,
            List<DocumentElement> links) {

        List<DocumentElement> mergeResult = new ArrayList<>();

        Map<String, DocumentElement> paragraphMap =
                paragraphs.stream()
                        .collect(
                                Collectors.toMap(
                                        DocumentElement::getParentId,
                                        Function.identity(),
                                        (existing, replacement) -> existing));

        Map<String, List<DocumentElement>> imageMap =
                images.stream()
                        .filter(element -> element.getParentId() != null)
                        .collect(Collectors.groupingBy(DocumentElement::getParentId));

        Map<String, List<DocumentElement>> linkMap =
                links.stream()
                        .filter(element -> element.getParentId() != null)
                        .collect(Collectors.groupingBy(DocumentElement::getParentId));

        for (DocumentElement heading : headings) {
            String parentId = heading.getElementId();

            mergeResult.add(heading);

            if (paragraphMap.containsKey(parentId)) {
                DocumentElement paragraph = paragraphMap.get(parentId);
                List<String> childIds = heading.getChildIds();
                childIds.add(paragraph.getElementId());
                heading.setChildIds(childIds);

                mergeResult.add(paragraph);
            } else {
                log.debug(
                        "No paragraph text found under heading '{}', which is expected for headings without body content.",
                        heading.getText());
            }

            if (imageMap.containsKey(parentId)) {
                List<DocumentElement> imageChild = imageMap.get(parentId);

                imageChild.forEach(
                        imageElement -> {
                            heading.getChildIds().add(imageElement.getElementId());
                            mergeResult.add(imageElement);
                        });
            }

            if (linkMap.containsKey(parentId)) {
                List<DocumentElement> linkChild = linkMap.get(parentId);

                linkChild.forEach(
                        linkElement -> {
                            heading.getChildIds().add(linkElement.getElementId());
                            mergeResult.add(linkElement);
                        });
            }
        }

        for (int i = 0; i < mergeResult.size(); i++) {
            mergeResult.get(i).setPositionIndex(i);
        }

        return mergeResult;
    }

    private String cleanParagraphText(String paragraphText) {
        return paragraphText.replace("\r\n", "\n");
    }

    /** Coordinate information for PDF elements */
    private static class CoordinateInfo {
        private final int pageNumber;
        private final float x;
        private final float y;
        private final String elementId;
        private final String text;

        public CoordinateInfo(int pageNumber, float x, float y, String elementId, String text) {
            this.pageNumber = pageNumber;
            this.x = x;
            this.y = y;
            this.elementId = elementId;
            this.text = text;
        }

        public CoordinateInfo(int pageNumber, float x, float y, String elementId) {
            this(pageNumber, x, y, elementId, null);
        }

        public int getPageNumber() {
            return pageNumber;
        }

        public float getX() {
            return x;
        }

        public float getY() {
            return y;
        }

        public String getElementId() {
            return elementId;
        }

        public String getText() {
            return text;
        }
    }

    private SeaTunnelRow toSeaTunnelRow(
            DocumentElement element,
            String sourceUri,
            String documentId,
            int chunkIndex,
            boolean appendRagMetadata) {
        SeaTunnelRow row = element.toSeaTunnelRow();
        if (!appendRagMetadata) {
            return row;
        }
        return new SeaTunnelRow(
                appendRagMetadata(row, sourceUri, documentId, chunkIndex, element.getText()));
    }

    private Object[] appendRagMetadata(
            SeaTunnelRow row, String sourceUri, String documentId, int chunkIndex, String text) {
        Object[] enriched = new Object[row.getArity() + RAG_METADATA_FIELD_NAMES.length];
        for (int i = 0; i < row.getArity(); i++) {
            enriched[i] = row.getField(i);
        }
        String contentHash = sha256Hex(text == null ? "" : text);
        String chunkId = "chunk_" + sha256Hex(documentId + ":" + chunkIndex + ":" + contentHash);
        enriched[row.getArity()] = sourceUri;
        enriched[row.getArity() + 1] = documentId;
        enriched[row.getArity() + 2] = chunkId;
        enriched[row.getArity() + 3] = chunkIndex;
        enriched[row.getArity() + 4] = contentHash;
        return enriched;
    }

    private static String buildDocumentId(String sourceUri) {
        return "doc_" + sha256Hex(sourceUri);
    }

    private static String normalizeSourceUri(String sourceUri) {
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

    /** Finds the paragraph boundary below an outline heading based on text position changes. */
    private static class ParagraphCoordinateExtractor {
        private final PDDocument document;

        public ParagraphCoordinateExtractor(PDDocument document) {
            this.document = document;
        }

        public float getParagraphEndY(CoordinateInfo outlineItemCoordinate, int pageNum)
                throws IOException {

            float[] lastTextFontSize = {0.0f};
            String[] lastTextFontName = {""};

            float[] paragraphEndY = {0.0f};

            PDFTextStripper textStripper =
                    new PDFTextStripper() {
                        @Override
                        protected void writeString(String text, List<TextPosition> textPositions)
                                throws IOException {

                            if (!textPositions.isEmpty()) {
                                TextPosition textPosition = textPositions.get(0);

                                if (textPosition.getEndY() <= outlineItemCoordinate.getY()) {

                                    if (paragraphEndY[0] > 0.0f) {
                                        return;
                                    }

                                    if (lastTextFontName[0].equals(StringUtils.EMPTY)) {
                                        lastTextFontName[0] = textPosition.getFont().getName();
                                    }

                                    if (textPosition.getFontSize() < lastTextFontSize[0]
                                            || !textPosition
                                                    .getFont()
                                                    .getName()
                                                    .equals(lastTextFontName[0])) {
                                        paragraphEndY[0] = textPosition.getEndY();
                                        return;
                                    } else {
                                        lastTextFontSize[0] = textPosition.getFontSize();
                                    }
                                }
                            }

                            super.writeString(text, textPositions);
                        }
                    };

            textStripper.setStartPage(pageNum);
            textStripper.setEndPage(pageNum);
            textStripper.getText(document);

            return paragraphEndY[0];
        }
    }

    /** Extracts image coordinates from PDF drawing operations for later heading assignment. */
    private static class PdfImageExtractor extends PDFStreamEngine {
        private final PDDocument document;
        @Getter private List<CoordinateInfo> imagesCoordinates = new ArrayList<>();

        /** @throws IOException If there is an error loading text stripper properties. */
        public PdfImageExtractor(PDDocument document) throws IOException {
            this.document = document;
        }

        /**
         * @param operator The operation to perform.
         * @param operands The list of arguments.
         * @throws IOException If there is an error processing the operation.
         */
        @Override
        protected void processOperator(Operator operator, List<COSBase> operands)
                throws IOException {
            PDPage currentPage = getCurrentPage();
            int pageNumber = document.getPages().indexOf(currentPage);

            String operation = operator.getName();
            if ("Do".equals(operation)) {
                COSName objectName = (COSName) operands.get(0);
                // get the PDF object
                PDXObject xobject = getResources().getXObject(objectName);
                // check if the object is an image object
                if (xobject instanceof PDImageXObject) {
                    Matrix ctmNew = getGraphicsState().getCurrentTransformationMatrix();

                    CoordinateInfo coordinateInfo =
                            new CoordinateInfo(
                                    pageNumber,
                                    ctmNew.getTranslateX(),
                                    ctmNew.getTranslateY(),
                                    null);

                    imagesCoordinates.add(coordinateInfo);
                } else if (xobject instanceof PDFormXObject) {
                    PDFormXObject form = (PDFormXObject) xobject;
                    showForm(form);
                }
            } else {
                super.processOperator(operator, operands);
            }
        }
    }
}
