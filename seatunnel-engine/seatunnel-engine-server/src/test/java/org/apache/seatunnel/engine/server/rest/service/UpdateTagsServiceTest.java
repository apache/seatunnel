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

        Map<String, String> tags = UpdateTagsService.toStringTags(request);
        assertEquals("legacy-member-tag", tags.get("uuid"));
        assertEquals(nestedTag.toString(), tags.get("tags"));
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
}
