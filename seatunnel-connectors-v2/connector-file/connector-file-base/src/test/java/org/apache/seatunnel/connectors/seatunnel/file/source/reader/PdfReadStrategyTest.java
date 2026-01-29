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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class PdfReadStrategyTest {

    @Test
    public void testReadPdfStrategy()
            throws URISyntaxException, IOException, FileConnectorException {
        URL resource = this.getClass().getResource("/pdf_read_strategy_test.pdf");

        String path = Paths.get(resource.toURI()).toString();
        PdfReadStrategy pdfReadStrategy = new PdfReadStrategy();
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

        // child size, containing first paragraph and sub heading elements
        Assertions.assertEquals(11, ((List<Object>) rows.get(0).getField(7)).size());

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

    private List<SeaTunnelRow> getHeadingElements(List<SeaTunnelRow> rows) {

        return rows.stream().filter(row -> row.getField(2) != null).collect(Collectors.toList());
    }
}
