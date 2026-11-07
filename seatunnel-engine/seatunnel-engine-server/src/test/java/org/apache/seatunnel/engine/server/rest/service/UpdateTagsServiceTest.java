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

package org.apache.seatunnel.engine.server.rest.service;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UpdateTagsServiceTest {

    @Test
    void shouldPreserveLegacyTagNames() {
        Map<String, Object> nestedTag = new HashMap<>();
        nestedTag.put("region", "us");
        Map<String, Object> request = new HashMap<>();
        request.put("uuid", "legacy-member-tag");
        request.put("tags", nestedTag);
        request.put("number", 42);
        request.put("empty", null);

        Map<String, String> tags = UpdateTagsService.toStringTags(request);
        assertEquals("legacy-member-tag", tags.get("uuid"));
        assertEquals(nestedTag.toString(), tags.get("tags"));
        assertEquals("42", tags.get("number"));
        assertEquals("", tags.get("empty"));
    }

    @Test
    void shouldExtractStructuredTags() {
        Map<String, Object> tags = new HashMap<>();
        tags.put("environment", "production");
        Map<String, Object> request = new HashMap<>();
        request.put("uuid", "target-member");
        request.put("tags", tags);

        assertEquals(tags, UpdateTagsService.extractStructuredTagParams(request));
    }

    @Test
    void shouldRejectStructuredRequestWithoutTagsObject() {
        Map<String, Object> request = new HashMap<>();
        request.put("uuid", "target-member");
        request.put("tags", "not-an-object");

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> UpdateTagsService.extractStructuredTagParams(request));
        assertTrue(error.getMessage().contains("tags field"));
    }

    @Test
    void shouldRejectNonStringStructuredTagValues() {
        Map<String, Object> tags = new HashMap<>();
        Map<String, Object> request = new HashMap<>();
        request.put("tags", tags);

        for (Object value : new Object[] {42, null, new HashMap<String, Object>()}) {
            tags.put("zone", value);
            IllegalArgumentException error =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> UpdateTagsService.extractStructuredTagParams(request));
            assertEquals("Tag values must be strings.", error.getMessage());
        }
    }

    @Test
    void shouldExcludeTagContentFromAuditSummary() {
        Map<String, String> tags = new HashMap<>();
        tags.put("api-token", "sensitive-value");
        tags.put("zone", "production");

        String summary = UpdateTagsService.summarizeTagsForAudit(tags);

        assertEquals("2 tag(s)", summary);
        assertFalse(summary.contains("api-token"));
        assertFalse(summary.contains("sensitive-value"));
        assertFalse(summary.contains("production"));
    }
}
