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

package org.apache.seatunnel.connectors.seatunnel.http.source;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FilenameExtractorTest {

    @Test
    public void testParseContentDispositionStandard() {
        String result =
                FilenameExtractor.parseContentDisposition("attachment; filename=\"report.pdf\"");
        Assertions.assertEquals("report.pdf", result);
    }

    @Test
    public void testParseContentDispositionWithoutQuotes() {
        String result =
                FilenameExtractor.parseContentDisposition("attachment; filename=report.pdf");
        Assertions.assertEquals("report.pdf", result);
    }

    @Test
    public void testParseContentDispositionUtf8Encoded() {
        String result =
                FilenameExtractor.parseContentDisposition(
                        "attachment; filename*=UTF-8''%E6%8A%A5%E5%91%8A.pdf");
        Assertions.assertEquals("报告.pdf", result);
    }

    @Test
    public void testParseContentDispositionNoFilename() {
        String result = FilenameExtractor.parseContentDisposition("attachment");
        Assertions.assertNull(result);
    }

    @Test
    public void testExtractFromUrlNormalPath() {
        String result = FilenameExtractor.extractFromUrl("http://example.com/files/demo.pdf");
        Assertions.assertEquals("demo.pdf", result);
    }

    @Test
    public void testExtractFromUrlNoExtension() {
        String result = FilenameExtractor.extractFromUrl("http://example.com/api/download");
        Assertions.assertNull(result);
    }

    @Test
    public void testExtractFromUrlWithQueryParams() {
        String result =
                FilenameExtractor.extractFromUrl(
                        "http://example.com/files/archive.zip?token=abc123");
        Assertions.assertEquals("archive.zip", result);
    }

    @Test
    public void testExtractFromUrlTrailingSlash() {
        String result = FilenameExtractor.extractFromUrl("http://example.com/files/");
        Assertions.assertNull(result);
    }

    @Test
    public void testExtractPriorityContentDispositionOverUrl() {
        String result =
                FilenameExtractor.extract(
                        "attachment; filename=\"report.pdf\"",
                        "http://example.com/files/other-name.pdf");
        Assertions.assertEquals("report.pdf", result);
    }

    @Test
    public void testExtractFallbackToUrl() {
        String result = FilenameExtractor.extract(null, "http://example.com/files/demo.zip");
        Assertions.assertEquals("demo.zip", result);
    }

    @Test
    public void testExtractDefaultFallback() {
        String result = FilenameExtractor.extract(null, "http://example.com/api/download");
        Assertions.assertTrue(result.startsWith("response-body-"));
    }
}
