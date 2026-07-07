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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the primary-key validation paths in {@link CouchbaseWriter}.
 *
 * <p>Two fail-fast guards are verified:
 *
 * <ol>
 *   <li>{@link CouchbaseWriter#validatePrimaryKeyFields} — rejects a configured key field that is
 *       absent from the row schema at construction time.
 *   <li>{@link CouchbaseWriter#buildDocumentKeyFrom} — rejects a schema-valid key field whose
 *       runtime row value is {@code null}, which would otherwise produce a silent {@code "null"}
 *       document key and cause data loss in upsert mode.
 * </ol>
 *
 * <p>Neither method requires a live Couchbase cluster connection.
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
    void buildDocumentKeyFrom_presentValue_returnsKey() {
        JsonObject doc = JsonObject.create().put("id", "row-1");
        String key = CouchbaseWriter.buildDocumentKeyFrom(new String[] {"id"}, doc);
        assertTrue(key.equals("row-1"), "Expected 'row-1' but got: " + key);
    }

    @Test
    void buildDocumentKeyFrom_compositeKey_joinsWithUnderscore() {
        JsonObject doc = JsonObject.create().put("id", "42").put("name", "alice");
        String key = CouchbaseWriter.buildDocumentKeyFrom(new String[] {"id", "name"}, doc);
        assertTrue(key.equals("42_alice"), "Expected '42_alice' but got: " + key);
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
}
