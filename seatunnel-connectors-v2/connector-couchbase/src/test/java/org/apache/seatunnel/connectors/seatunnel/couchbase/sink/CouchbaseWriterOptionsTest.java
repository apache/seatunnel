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

package org.apache.seatunnel.connectors.seatunnel.couchbase.sink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CouchbaseWriterOptionsTest {

    // Serialized default options from before readyTimeout was added (serialVersionUID = 1).
    private static final String LEGACY_OPTIONS =
            "rO0ABXNyAE9vcmcuYXBhY2hlLnNlYXR1bm5lbC5jb25uZWN0b3JzLnNlYXR1bm5lbC5jb3VjaGJhc2Uuc2luay5Db3VjaGJhc2VXcml0ZXJPcHRpb25zAAAAAAAAAAECAAtJAAlmbHVzaFNpemVKAA1yZXRyeUludGVydmFsSQAIcmV0cnlNYXhaAAx1cHNlcnRFbmFibGVMAAZidWNrZXR0ABJMamF2YS9sYW5nL1N0cmluZztMAApjb2xsZWN0aW9ucQB+AAFMABBjb25uZWN0aW9uU3RyaW5ncQB+AAFMAAhwYXNzd29yZHEAfgABWwAKcHJpbWFyeUtleXQAE1tMamF2YS9sYW5nL1N0cmluZztMAAVzY29wZXEAfgABTAAIdXNlcm5hbWVxAH4AAXhwAAAD6AAAAAAAAAPoAAAAAwBwcHBwdXIAE1tMamF2YS5sYW5nLlN0cmluZzut0lbn6R17RwIAAHhwAAAAAHQACF9kZWZhdWx0cA==";

    @Test
    void testDefaultReadinessTimeout() {
        assertEquals(30, CouchbaseWriterOptions.builder().build().getReadyTimeout());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testBuilderRejectsNonPositiveReadinessTimeout(int timeout) {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> CouchbaseWriterOptions.builder().withReadyTimeout(timeout));
        assertTrue(error.getMessage().contains("ready.timeout"));
    }

    @Test
    void testConfiguredReadinessTimeoutSurvivesSerialization() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(CouchbaseWriterOptions.builder().withReadyTimeout(60).build());
        }
        assertEquals(60, deserialize(bytes.toByteArray()).getReadyTimeout());
    }

    @Test
    void testLegacySerializedOptionsRetainThirtySecondTimeout() throws Exception {
        CouchbaseWriterOptions options = deserialize(Base64.getDecoder().decode(LEGACY_OPTIONS));
        assertEquals(30, options.getReadyTimeout());
        assertEquals(3, options.getRetryMax());
        assertEquals("_default", options.getScope());
    }

    private CouchbaseWriterOptions deserialize(byte[] bytes) throws Exception {
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (CouchbaseWriterOptions) input.readObject();
        }
    }
}
