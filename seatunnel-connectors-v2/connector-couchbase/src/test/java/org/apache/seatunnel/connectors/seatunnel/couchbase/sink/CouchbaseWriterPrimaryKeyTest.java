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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.couchbase.exception.CouchbaseConnectorException;

import org.junit.jupiter.api.Test;

import com.couchbase.client.java.json.JsonObject;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the primary-key validation and document-key assembly paths in {@link
 * CouchbaseWriter}.
 *
 * <p>Three groups of assertions are covered:
 *
 * <ol>
 *   <li>{@link CouchbaseWriter#validatePrimaryKeyFields} — rejects a configured key field that is
 *       absent from the row schema at construction time.
 *   <li>{@link CouchbaseWriter#buildDocumentKeyFrom} — rejects a schema-valid key field whose
 *       runtime row value is {@code null}, which would otherwise produce a silent {@code "null"}
 *       document key and cause data loss in upsert mode.
 *   <li>Collision regression — verifies that distinct composite-key tuples that would collide under
 *       a plain underscore-join are correctly distinguished by the length-prefixed encoding.
 * </ol>
 *
 * <p>None of these tests require a live Couchbase cluster connection.
 */
class CouchbaseWriterPrimaryKeyTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "value"},
                    new SeaTunnelDataType<?>[] {
                        BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.LONG_TYPE
                    });

    // -------------------------------------------------------------------------
    // validatePrimaryKeyFields — schema-check at writer construction
    // -------------------------------------------------------------------------

    @Test
    void validatePrimaryKeyFields_validSingleField_doesNotThrow() {
        assertDoesNotThrow(
                () -> CouchbaseWriter.validatePrimaryKeyFields(new String[] {"id"}, ROW_TYPE));
    }

    @Test
    void validatePrimaryKeyFields_validCompositeKey_doesNotThrow() {
        assertDoesNotThrow(
                () ->
                        CouchbaseWriter.validatePrimaryKeyFields(
                                new String[] {"id", "name"}, ROW_TYPE));
    }

    @Test
    void validatePrimaryKeyFields_emptyArray_doesNotThrow() {
        assertDoesNotThrow(() -> CouchbaseWriter.validatePrimaryKeyFields(new String[0], ROW_TYPE));
    }

    @Test
    void validatePrimaryKeyFields_nullArray_doesNotThrow() {
        assertDoesNotThrow(() -> CouchbaseWriter.validatePrimaryKeyFields(null, ROW_TYPE));
    }

    @Test
    void validatePrimaryKeyFields_fieldNotInSchema_throwsWithFieldName() {
        CouchbaseConnectorException ex =
                assertThrows(
                        CouchbaseConnectorException.class,
                        () ->
                                CouchbaseWriter.validatePrimaryKeyFields(
                                        new String[] {"typo_id"}, ROW_TYPE));
        assertTrue(
                ex.getMessage().contains("typo_id"),
                "Exception message must identify the invalid field; got: " + ex.getMessage());
    }

    @Test
    void validatePrimaryKeyFields_oneValidOneInvalid_throwsWithInvalidFieldName() {
        // Only the missing field should appear in the error message.
        CouchbaseConnectorException ex =
                assertThrows(
                        CouchbaseConnectorException.class,
                        () ->
                                CouchbaseWriter.validatePrimaryKeyFields(
                                        new String[] {"id", "unknown_field"}, ROW_TYPE));
        assertTrue(
                ex.getMessage().contains("unknown_field"),
                "Exception message must identify the missing field; got: " + ex.getMessage());
    }

    // -------------------------------------------------------------------------
    // buildDocumentKeyFrom — null-value guard at document-key assembly
    // -------------------------------------------------------------------------

    @Test
    void buildDocumentKeyFrom_presentValue_returnsLengthPrefixedKey() {
        JsonObject doc = JsonObject.create().put("id", "row-1");
        // Expected: length(5):"row-1"
        String key = CouchbaseWriter.buildDocumentKeyFrom(new String[] {"id"}, doc);
        assertTrue(key.equals("5:row-1"), "Expected '5:row-1' but got: " + key);
    }

    @Test
    void buildDocumentKeyFrom_compositeKey_usesLengthPrefixedEncoding() {
        JsonObject doc = JsonObject.create().put("id", "42").put("name", "alice");
        // Expected: "2:42#5:alice"  (not "42_alice")
        String key = CouchbaseWriter.buildDocumentKeyFrom(new String[] {"id", "name"}, doc);
        assertTrue(key.equals("2:42#5:alice"), "Expected '2:42#5:alice' but got: " + key);
    }

    @Test
    void buildDocumentKeyFrom_nullValue_throwsWithFieldName() {
        // doc.get("id") returns null because the key was never put into the JsonObject.
        JsonObject doc = JsonObject.create();

        CouchbaseConnectorException ex =
                assertThrows(
                        CouchbaseConnectorException.class,
                        () -> CouchbaseWriter.buildDocumentKeyFrom(new String[] {"id"}, doc));
        assertTrue(
                ex.getMessage().contains("id"),
                "Exception message must name the null primary-key field; got: " + ex.getMessage());
    }

    @Test
    void buildDocumentKeyFrom_nullFieldInCompositeKey_throwsWithFieldName() {
        // 'name' is absent → null value; 'id' is present.
        JsonObject doc = JsonObject.create().put("id", "10");

        CouchbaseConnectorException ex =
                assertThrows(
                        CouchbaseConnectorException.class,
                        () ->
                                CouchbaseWriter.buildDocumentKeyFrom(
                                        new String[] {"id", "name"}, doc));
        assertTrue(
                ex.getMessage().contains("name"),
                "Exception message must name the null field; got: " + ex.getMessage());
    }

    @Test
    void buildDocumentKeyFrom_emptyPrimaryKey_returnsNonNullUuid() {
        JsonObject doc = JsonObject.create();
        String key = CouchbaseWriter.buildDocumentKeyFrom(new String[0], doc);
        assertTrue(key != null && !key.isEmpty(), "Expected a UUID fallback but got: " + key);
    }

    // -------------------------------------------------------------------------
    // Collision regression — length-prefixed encoding must distinguish tuples
    // that a plain underscore-join would conflate.
    // -------------------------------------------------------------------------

    /**
     * Regression for SEZ9: ("a_b", "c") and ("a", "b_c") both produce "a_b_c" with a naive
     * underscore-join. The length-prefixed encoding must yield distinct document IDs.
     */
    @Test
    void buildDocumentKeyFrom_collisionRegressionUnderscoreInValue_keysAreDistinct() {
        // Tuple 1: first component contains an underscore
        JsonObject doc1 = JsonObject.create().put("col1", "a_b").put("col2", "c");
        // Tuple 2: second component contains an underscore
        JsonObject doc2 = JsonObject.create().put("col1", "a").put("col2", "b_c");

        String key1 = CouchbaseWriter.buildDocumentKeyFrom(new String[] {"col1", "col2"}, doc1);
        String key2 = CouchbaseWriter.buildDocumentKeyFrom(new String[] {"col1", "col2"}, doc2);

        assertNotEquals(
                key1,
                key2,
                "Composite keys ('a_b','c') and ('a','b_c') must not produce the same document ID."
                        + " key1="
                        + key1
                        + ", key2="
                        + key2);
    }

    /** Verify the exact encoded forms so the regression is fully pinned. */
    @Test
    void buildDocumentKeyFrom_collisionRegressionUnderscoreInValue_exactEncodings() {
        JsonObject doc1 = JsonObject.create().put("col1", "a_b").put("col2", "c");
        JsonObject doc2 = JsonObject.create().put("col1", "a").put("col2", "b_c");

        String key1 = CouchbaseWriter.buildDocumentKeyFrom(new String[] {"col1", "col2"}, doc1);
        String key2 = CouchbaseWriter.buildDocumentKeyFrom(new String[] {"col1", "col2"}, doc2);

        assertTrue(key1.equals("3:a_b#1:c"), "Expected '3:a_b#1:c' but got: " + key1);
        assertTrue(key2.equals("1:a#3:b_c"), "Expected '1:a#3:b_c' but got: " + key2);
    }

    /**
     * Empty-string components must also be distinguishable — e.g. ("", "ab") vs ("a", "b") vs
     * ("ab", "").
     */
    @Test
    void buildDocumentKeyFrom_collisionRegressionEmptyComponents_keysAreDistinct() {
        JsonObject docEmpty1 = JsonObject.create().put("col1", "").put("col2", "ab");
        JsonObject docMid = JsonObject.create().put("col1", "a").put("col2", "b");
        JsonObject docEmpty2 = JsonObject.create().put("col1", "ab").put("col2", "");

        String keyEmpty1 =
                CouchbaseWriter.buildDocumentKeyFrom(new String[] {"col1", "col2"}, docEmpty1);
        String keyMid = CouchbaseWriter.buildDocumentKeyFrom(new String[] {"col1", "col2"}, docMid);
        String keyEmpty2 =
                CouchbaseWriter.buildDocumentKeyFrom(new String[] {"col1", "col2"}, docEmpty2);

        assertNotEquals(keyEmpty1, keyMid, "('','ab') must differ from ('a','b')");
        assertNotEquals(keyMid, keyEmpty2, "('a','b') must differ from ('ab','')");
        assertNotEquals(keyEmpty1, keyEmpty2, "('','ab') must differ from ('ab','')");
    }
}
