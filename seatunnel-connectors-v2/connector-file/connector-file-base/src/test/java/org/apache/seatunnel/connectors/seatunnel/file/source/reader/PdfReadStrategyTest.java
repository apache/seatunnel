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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.DocumentElement;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class PdfReadStrategyTest {

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
    private static final String[] RAG_FIELD_NAMES = {
        "source_uri", "document_id", "chunk_id", "chunk_index", "content_hash"
    };

    @Test
    public void testReadPdfStrategy()
            throws URISyntaxException, IOException, FileConnectorException {
        URL resource = this.getClass().getResource("/pdf_read_strategy_test.pdf");

        String path = Paths.get(resource.toURI()).toString();
        PdfReadStrategy pdfReadStrategy = new PdfReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        pdfReadStrategy.init(localConf);
        TempCollector tempCollector = new TempCollector();
        pdfReadStrategy.read(path, "", tempCollector);

        List<SeaTunnelRow> rows = tempCollector.getRows();

        List<SeaTunnelRow> headingElements = getHeadingElements(rows);

        // verify heading elements count
        Assertions.assertEquals(11, headingElements.size());

        Assertions.assertEquals("heading", rows.get(0).getField(1));
        Assertions.assertEquals(1, rows.get(0).getField(2));
        Assertions.assertEquals(
                "The Essential Guide to Groceries: Shopping, Storing, and Enjoying Food at Home",
                rows.get(0).getField(3));
        Assertions.assertEquals(1, rows.get(0).getField(4));
        Assertions.assertEquals(0, rows.get(0).getField(5));
        Assertions.assertNull(rows.get(0).getField(6));

        // child_ids must be a String[] array (not a List or comma-separated String)
        String[] childIds = (String[]) rows.get(0).getField(7);
        Assertions.assertNotNull(childIds);
        Assertions.assertEquals(11, childIds.length);

        // check paragraph
        String expectedParagraph =
                "Groceries play a vital role in daily life, touching every aspect of health, convenience, and enjoyment.\n"
                        + "This comprehensive guide covers all things groceries—from what to shop for, strategies to save money, storage tips, and even how groceries have changed in the\n"
                        + "modern era.";

        Assertions.assertEquals(
                expectedParagraph.length(), String.valueOf(rows.get(1).getField(3)).length());
        Assertions.assertEquals(expectedParagraph, rows.get(1).getField(3));

        // check link element type
        int lastIndex = rows.size() - 1;
        Assertions.assertEquals("link", rows.get(lastIndex).getField(1));
        // check link element text
        Assertions.assertEquals("https://example.com", rows.get(lastIndex).getField(3));
        // check link element page
        Assertions.assertEquals(3, rows.get(lastIndex).getField(4));
    }

    @Test
    public void testReadInvalidPdfThrowsException() {
        PdfReadStrategy strategy = new PdfReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        strategy.init(localConf);
        TempCollector collector = new TempCollector();
        Assertions.assertThrows(
                FileConnectorException.class,
                () -> strategy.read("/nonexistent/path/file.pdf", "", collector));
    }

    @Test
    public void testReadPdfWithRagMetadata()
            throws URISyntaxException, IOException, FileConnectorException {
        URL resource = this.getClass().getResource("/pdf_read_strategy_test.pdf");
        String path = Paths.get(resource.toURI()).toString();
        PdfReadStrategy pdfReadStrategy = createPdfReadStrategy(true);
        SeaTunnelRowType rowType = pdfReadStrategy.getSeaTunnelRowTypeInfo(path);
        TempCollector firstCollector = new TempCollector();
        pdfReadStrategy.read(path, "", firstCollector);

        Assertions.assertArrayEquals(
                concat(DEFAULT_FIELD_NAMES, RAG_FIELD_NAMES), rowType.getFieldNames());
        Assertions.assertFalse(firstCollector.getRows().isEmpty());
        Assertions.assertEquals(13, firstCollector.getRows().get(0).getArity());
        Assertions.assertEquals(path, firstCollector.getRows().get(0).getField(8));
        Assertions.assertTrue(
                String.valueOf(firstCollector.getRows().get(0).getField(9)).startsWith("doc_"));
        Assertions.assertTrue(
                String.valueOf(firstCollector.getRows().get(0).getField(10)).startsWith("chunk_"));
        Assertions.assertEquals(1, firstCollector.getRows().get(0).getField(11));
        Assertions.assertEquals(
                64, String.valueOf(firstCollector.getRows().get(0).getField(12)).length());

        PdfReadStrategy secondReadStrategy = createPdfReadStrategy(true);
        TempCollector secondCollector = new TempCollector();
        secondReadStrategy.read(path, "", secondCollector);

        for (int fieldIndex = 8; fieldIndex < 13; fieldIndex++) {
            Assertions.assertEquals(
                    firstCollector.getRows().get(0).getField(fieldIndex),
                    secondCollector.getRows().get(0).getField(fieldIndex));
        }
    }

    @Test
    public void testReadPdfWithRagMetadataNormalizesFileUri()
            throws IOException, FileConnectorException {
        Path tempPdf = createSimplePdf("Title in file uri pdf");
        try {
            PdfReadStrategy pdfReadStrategy = createPdfReadStrategy(true);
            TempCollector tempCollector = new TempCollector();
            pdfReadStrategy.read(tempPdf.toUri().toString(), "", tempCollector);

            PdfReadStrategy expectedReadStrategy = createPdfReadStrategy(true);
            TempCollector expectedCollector = new TempCollector();
            expectedReadStrategy.read(tempPdf.toString(), "", expectedCollector);

            Assertions.assertEquals(tempPdf.toString(), tempCollector.getRows().get(0).getField(8));
            for (int fieldIndex = 8; fieldIndex < 11; fieldIndex++) {
                Assertions.assertEquals(
                        expectedCollector.getRows().get(0).getField(fieldIndex),
                        tempCollector.getRows().get(0).getField(fieldIndex));
            }
        } finally {
            Files.deleteIfExists(tempPdf);
        }
    }

    @Test
    public void testChildIdsIsArrayType() throws URISyntaxException, IOException {
        URL resource = this.getClass().getResource("/pdf_read_strategy_test.pdf");
        String path = Paths.get(resource.toURI()).toString();
        PdfReadStrategy pdfReadStrategy = new PdfReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        pdfReadStrategy.init(localConf);
        TempCollector tempCollector = new TempCollector();
        pdfReadStrategy.read(path, "", tempCollector);

        for (SeaTunnelRow row : tempCollector.getRows()) {
            Object childIdsField = row.getField(7);
            Assertions.assertTrue(
                    childIdsField == null || childIdsField instanceof String[],
                    "child_ids must be null or String[], got: "
                            + (childIdsField == null ? "null" : childIdsField.getClass()));
        }
    }

    @Test
    public void testImageElementsHaveText() throws URISyntaxException, IOException {
        URL resource = this.getClass().getResource("/pdf_read_strategy_test.pdf");
        String path = Paths.get(resource.toURI()).toString();
        PdfReadStrategy pdfReadStrategy = new PdfReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        pdfReadStrategy.init(localConf);
        TempCollector tempCollector = new TempCollector();
        pdfReadStrategy.read(path, "", tempCollector);

        List<SeaTunnelRow> imageRows =
                tempCollector.getRows().stream()
                        .filter(row -> "image".equals(row.getField(1)))
                        .collect(Collectors.toList());

        for (SeaTunnelRow imageRow : imageRows) {
            String text = (String) imageRow.getField(3);
            Assertions.assertNotNull(text, "image element must have text");
            Assertions.assertTrue(
                    text.startsWith("image_page_"), "image text must start with 'image_page_'");
        }
    }

    @Test
    public void testNoOutlinePdfWithImagesExtractsImageElements() throws IOException {
        Path tempPdf = createNoOutlinePdfWithImage();
        try {
            PdfReadStrategy pdfReadStrategy = new PdfReadStrategy();
            LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
            pdfReadStrategy.init(localConf);
            TempCollector tempCollector = new TempCollector();
            pdfReadStrategy.read(tempPdf.toString(), "", tempCollector);

            List<SeaTunnelRow> rows = tempCollector.getRows();
            Assertions.assertFalse(rows.isEmpty(), "Should produce at least one element");

            List<SeaTunnelRow> paragraphs =
                    rows.stream()
                            .filter(row -> "paragraph".equals(row.getField(1)))
                            .collect(Collectors.toList());
            Assertions.assertFalse(paragraphs.isEmpty(), "Should have paragraph elements");

            List<SeaTunnelRow> images =
                    rows.stream()
                            .filter(row -> "image".equals(row.getField(1)))
                            .collect(Collectors.toList());
            Assertions.assertFalse(
                    images.isEmpty(), "Should have image elements for no-outline PDF");

            for (SeaTunnelRow imageRow : images) {
                String text = (String) imageRow.getField(3);
                Assertions.assertNotNull(text, "image element must have text");
                Assertions.assertTrue(
                        text.startsWith("image_page_"), "image text must start with 'image_page_'");

                Integer pageNumber = (Integer) imageRow.getField(4);
                Assertions.assertNotNull(pageNumber, "image element must have page number");
                Assertions.assertTrue(pageNumber >= 1, "page number must be >= 1");

                // no-outline images should have null parentId
                Assertions.assertNull(
                        imageRow.getField(6), "no-outline image must have null parentId");
            }

            // elements should be sorted by page number
            for (int i = 1; i < rows.size(); i++) {
                int prevPage = (Integer) rows.get(i - 1).getField(4);
                int currPage = (Integer) rows.get(i).getField(4);
                Assertions.assertTrue(
                        currPage >= prevPage, "elements should be sorted by page number");
            }
        } finally {
            Files.deleteIfExists(tempPdf);
        }
    }

    @Test
    public void testMergeElementsIgnoresImageAndLinkWithoutParentId()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        PdfReadStrategy strategy = new PdfReadStrategy();
        Method mergeElementsMethod =
                PdfReadStrategy.class.getDeclaredMethod(
                        "mergeElements", List.class, List.class, List.class, List.class);
        mergeElementsMethod.setAccessible(true);

        DocumentElement heading = new DocumentElement("heading", "Section 1");
        heading.setChildIds(new ArrayList<>());

        DocumentElement image = new DocumentElement("image", "image_page_1_pos_(100,100)");
        image.setParentId(null);

        DocumentElement link = new DocumentElement("link", "https://example.com/orphan");
        link.setParentId(null);

        @SuppressWarnings("unchecked")
        List<DocumentElement> mergedElements =
                (List<DocumentElement>)
                        mergeElementsMethod.invoke(
                                strategy,
                                Collections.singletonList(heading),
                                new ArrayList<>(),
                                Collections.singletonList(image),
                                Collections.singletonList(link));

        Assertions.assertEquals(1, mergedElements.size());
        Assertions.assertEquals("heading", mergedElements.get(0).getElementType());
    }

    @Test
    public void testReadDeletesTemporaryPdfCopy()
            throws IOException, URISyntaxException, FileConnectorException {
        URL resource = this.getClass().getResource("/pdf_read_strategy_test.pdf");
        String path = Paths.get(resource.toURI()).toString();
        TrackingPdfReadStrategy pdfReadStrategy = new TrackingPdfReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        pdfReadStrategy.init(localConf);

        TempCollector tempCollector = new TempCollector();
        pdfReadStrategy.read(path, "", tempCollector);

        Assertions.assertNotNull(pdfReadStrategy.getCreatedTempPdfPath());
        Assertions.assertTrue(pdfReadStrategy.wasTempPdfDeleted());
        Assertions.assertFalse(Files.exists(pdfReadStrategy.getCreatedTempPdfPath()));
    }

    private Path createNoOutlinePdfWithImage() throws IOException {
        Path tempFile = Files.createTempFile("no_outline_pdf_test_", ".pdf");
        try (PDDocument document = new PDDocument()) {
            PDPage page = new PDPage(PDRectangle.A4);
            document.addPage(page);

            // Create a simple test image (red 50x50 square)
            BufferedImage bufferedImage = new BufferedImage(50, 50, BufferedImage.TYPE_INT_RGB);
            for (int x = 0; x < 50; x++) {
                for (int y = 0; y < 50; y++) {
                    bufferedImage.setRGB(x, y, 0xFF0000);
                }
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(bufferedImage, "png", baos);
            PDImageXObject pdImage =
                    PDImageXObject.createFromByteArray(document, baos.toByteArray(), "test.png");

            try (PDPageContentStream contentStream = new PDPageContentStream(document, page)) {
                contentStream.beginText();
                contentStream.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12);
                contentStream.newLineAtOffset(50, 700);
                contentStream.showText("This is a PDF without outline containing an image.");
                contentStream.endText();

                contentStream.drawImage(pdImage, 50, 500, 100, 100);
            }

            // No outline is added — this PDF has no bookmarks
            document.save(tempFile.toFile());
        }
        return tempFile;
    }

    private Path createSimplePdf(String text) throws IOException {
        Path tempFile = Files.createTempFile("pdf_rag_metadata_test_", ".pdf");
        try (PDDocument document = new PDDocument()) {
            PDPage page = new PDPage(PDRectangle.A4);
            document.addPage(page);
            try (PDPageContentStream contentStream = new PDPageContentStream(document, page)) {
                contentStream.beginText();
                contentStream.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12);
                contentStream.newLineAtOffset(50, 700);
                contentStream.showText(text);
                contentStream.endText();
            }
            document.save(tempFile.toFile());
        }
        return tempFile;
    }

    private List<SeaTunnelRow> getHeadingElements(List<SeaTunnelRow> rows) {
        return rows.stream().filter(row -> row.getField(2) != null).collect(Collectors.toList());
    }

    private PdfReadStrategy createPdfReadStrategy(boolean ragMetadataEnabled) {
        PdfReadStrategy pdfReadStrategy = new PdfReadStrategy();
        pdfReadStrategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
        if (ragMetadataEnabled) {
            pdfReadStrategy.setPluginConfig(
                    ConfigFactory.parseString("pdf_rag_metadata_enabled = true"));
        }
        return pdfReadStrategy;
    }

    private static String[] concat(String[] left, String[] right) {
        String[] result = new String[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    public static class LocalConf extends HadoopConf {
        private static final String HDFS_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
        private static final String SCHEMA = "file";

        public LocalConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return HDFS_IMPL;
        }

        @Override
        public String getSchema() {
            return SCHEMA;
        }
    }

    private static class TrackingPdfReadStrategy extends PdfReadStrategy {
        private Path createdTempPdfPath;
        private boolean tempPdfDeleted;

        @Override
        Path createTempPdfPath() throws IOException {
            createdTempPdfPath = super.createTempPdfPath();
            return createdTempPdfPath;
        }

        @Override
        void deleteTempPdfPath(Path tempPdfPath) throws IOException {
            super.deleteTempPdfPath(tempPdfPath);
            tempPdfDeleted = true;
        }

        Path getCreatedTempPdfPath() {
            return createdTempPdfPath;
        }

        boolean wasTempPdfDeleted() {
            return tempPdfDeleted;
        }
    }
}
