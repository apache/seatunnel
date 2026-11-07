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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.TimeZone;

public class DuckDBJdbcRowConverterTest {

    /**
     * Stubs both JDBC routes of an alias column with the same value, the way a plain timestamp
     * value outside a DST transition is reported.
     */
    private static void stubAliasValue(ResultSet resultSet, int column, Timestamp value)
            throws Exception {
        Mockito.when(
                        resultSet.getTimestamp(
                                ArgumentMatchers.eq(column), ArgumentMatchers.any(Calendar.class)))
                .thenReturn(value);
        Mockito.when(resultSet.getTimestamp(column)).thenReturn(value);
    }

    @ResourceLock("java.util.TimeZone.default")
    @Test
    public void testAliasMetadataIsReadOncePerResultSet() throws Exception {
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            DuckDBJdbcRowConverter converter = new DuckDBJdbcRowConverter();
            ResultSet resultSet = Mockito.mock(ResultSet.class);
            ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
            Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
            // Three timestamp columns with different declared types: one metadata read has to
            // classify all of them, including a type name reported in a different case.
            Mockito.when(metadata.getColumnCount()).thenReturn(3);
            Mockito.when(metadata.getColumnTypeName(1)).thenReturn("TIMESTAMP");
            Mockito.when(metadata.getColumnTypeName(2)).thenReturn("TIMESTAMP_S");
            Mockito.when(metadata.getColumnTypeName(3)).thenReturn("timestamp_ms");
            LocalDateTime plain = LocalDateTime.of(2024, 1, 1, 12, 34, 56);
            Mockito.when(resultSet.getObject(1, LocalDateTime.class)).thenReturn(plain);
            Timestamp alias = Timestamp.from(Instant.parse("2024-06-01T10:00:00Z"));
            stubAliasValue(resultSet, 2, alias);
            stubAliasValue(resultSet, 3, alias);

            for (int row = 0; row < 3; row++) {
                String message = "row " + row;
                Assertions.assertEquals(plain, converter.readTimestamp(resultSet, 1), message);
                Assertions.assertEquals(
                        LocalDateTime.of(2024, 6, 1, 10, 0),
                        converter.readTimestamp(resultSet, 2),
                        message);
                Assertions.assertEquals(
                        LocalDateTime.of(2024, 6, 1, 10, 0),
                        converter.readTimestamp(resultSet, 3),
                        message);
            }

            // The per-column alias decisions are computed once per result set, never per value.
            Mockito.verify(resultSet, Mockito.times(1)).getMetaData();
            Mockito.verify(metadata, Mockito.times(1)).getColumnCount();
            for (int column = 1; column <= 3; column++) {
                Mockito.verify(metadata, Mockito.times(1)).getColumnTypeName(column);
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @ResourceLock("java.util.TimeZone.default")
    @Test
    public void testAliasDecisionsAreRebuiltWhenTheResultSetChanges() throws Exception {
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            DuckDBJdbcRowConverter converter = new DuckDBJdbcRowConverter();
            Timestamp alias = Timestamp.from(Instant.parse("2024-06-01T10:00:00Z"));
            LocalDateTime expected = LocalDateTime.of(2024, 6, 1, 10, 0);

            ResultSet aliasResultSet = Mockito.mock(ResultSet.class);
            ResultSetMetaData aliasMetadata = Mockito.mock(ResultSetMetaData.class);
            Mockito.when(aliasResultSet.getMetaData()).thenReturn(aliasMetadata);
            Mockito.when(aliasMetadata.getColumnCount()).thenReturn(1);
            Mockito.when(aliasMetadata.getColumnTypeName(1)).thenReturn("TIMESTAMP_NS");
            stubAliasValue(aliasResultSet, 1, alias);

            // A different result set declares the same column index as a standard TIMESTAMP.
            ResultSet typedResultSet = Mockito.mock(ResultSet.class);
            ResultSetMetaData typedMetadata = Mockito.mock(ResultSetMetaData.class);
            Mockito.when(typedResultSet.getMetaData()).thenReturn(typedMetadata);
            Mockito.when(typedMetadata.getColumnCount()).thenReturn(1);
            Mockito.when(typedMetadata.getColumnTypeName(1)).thenReturn("TIMESTAMP");
            Mockito.when(typedResultSet.getObject(1, LocalDateTime.class)).thenReturn(expected);

            Assertions.assertEquals(expected, converter.readTimestamp(aliasResultSet, 1));
            // A new result set must not inherit the alias decision of the previous one.
            Assertions.assertEquals(expected, converter.readTimestamp(typedResultSet, 1));
            Mockito.verify(typedResultSet, Mockito.never()).getTimestamp(1);
            Mockito.verify(typedResultSet, Mockito.never())
                    .getTimestamp(ArgumentMatchers.eq(1), ArgumentMatchers.any(Calendar.class));
            // Switching back must rebuild instead of reusing the newer result set's metadata.
            Assertions.assertEquals(expected, converter.readTimestamp(aliasResultSet, 1));
            Mockito.verify(aliasResultSet, Mockito.times(2)).getMetaData();
            Mockito.verify(typedResultSet, Mockito.times(1)).getMetaData();
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    public void testMissingMetadataKeepsTheTypedReadAndIsNotCached() throws Exception {
        DuckDBJdbcRowConverter converter = new DuckDBJdbcRowConverter();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getMetaData()).thenReturn(null);
        LocalDateTime expected = LocalDateTime.of(2024, 1, 1, 12, 34, 56);
        Mockito.when(resultSet.getObject(1, LocalDateTime.class)).thenReturn(expected);

        Assertions.assertEquals(expected, converter.readTimestamp(resultSet, 1));
        Assertions.assertEquals(expected, converter.readTimestamp(resultSet, 1));

        // Metadata that cannot be read must not be remembered as "this column is no alias".
        Mockito.verify(resultSet, Mockito.times(2)).getMetaData();
        Mockito.verify(resultSet, Mockito.never()).getTimestamp(1);
    }

    @ResourceLock("java.util.TimeZone.default")
    @Test
    public void testPreEpochAliasValueKeepsTheLegacyGetter() throws Exception {
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            DuckDBJdbcRowConverter converter = new DuckDBJdbcRowConverter();
            ResultSet resultSet = Mockito.mock(ResultSet.class);
            ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
            Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
            Mockito.when(metadata.getColumnCount()).thenReturn(1);
            Mockito.when(metadata.getColumnTypeName(1)).thenReturn("TIMESTAMP_S");
            Mockito.when(
                            resultSet.getTimestamp(
                                    ArgumentMatchers.eq(1), ArgumentMatchers.any(Calendar.class)))
                    .thenReturn(Timestamp.from(Instant.parse("1918-03-31T02:30:00Z")));
            Mockito.when(resultSet.getTimestamp(1))
                    .thenReturn(Timestamp.from(Instant.parse("1918-03-31T03:30:00Z")));

            // Before the epoch the legacy getter wins even when the two routes disagree, so the
            // pre-1970 DST-gap and Gregorian-cutover limitations stay exactly as before (deferred).
            Assertions.assertEquals(
                    LocalDateTime.of(1918, 3, 31, 3, 30), converter.readTimestamp(resultSet, 1));
            // The alias column still never reaches the typed getter that rejects these types.
            Mockito.verify(resultSet, Mockito.never()).getObject(1, LocalDateTime.class);

            // A positive epoch keeps the reconciled route instead of the legacy value.
            Mockito.when(
                            resultSet.getTimestamp(
                                    ArgumentMatchers.eq(1), ArgumentMatchers.any(Calendar.class)))
                    .thenReturn(Timestamp.from(Instant.parse("2024-06-01T10:00:00Z")));
            Mockito.when(resultSet.getTimestamp(1))
                    .thenReturn(Timestamp.from(Instant.parse("2024-06-01T11:00:00Z")));
            Assertions.assertEquals(
                    LocalDateTime.of(2024, 6, 1, 11, 0), converter.readTimestamp(resultSet, 1));
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @ResourceLock("java.util.TimeZone.default")
    @Test
    public void testConverterSerializationExcludesTheCachedResultSet() throws Exception {
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            DuckDBJdbcRowConverter converter = new DuckDBJdbcRowConverter();
            ResultSet resultSet = Mockito.mock(ResultSet.class);
            ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
            Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
            Mockito.when(metadata.getColumnCount()).thenReturn(1);
            Mockito.when(metadata.getColumnTypeName(1)).thenReturn("TIMESTAMP_MS");
            stubAliasValue(resultSet, 1, Timestamp.from(Instant.parse("2024-06-01T10:00:00Z")));
            // Populate the per-result-set cache before the converter is shipped to a worker.
            Assertions.assertEquals(
                    LocalDateTime.of(2024, 6, 1, 10, 0), converter.readTimestamp(resultSet, 1));

            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(converter);
            }
            DuckDBJdbcRowConverter copy;
            try (ObjectInputStream in =
                    new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                copy = (DuckDBJdbcRowConverter) in.readObject();
            }

            // The deserialized converter rebuilds the cache from the result set it is given.
            ResultSet reopened = Mockito.mock(ResultSet.class);
            ResultSetMetaData reopenedMetadata = Mockito.mock(ResultSetMetaData.class);
            Mockito.when(reopened.getMetaData()).thenReturn(reopenedMetadata);
            Mockito.when(reopenedMetadata.getColumnCount()).thenReturn(1);
            Mockito.when(reopenedMetadata.getColumnTypeName(1)).thenReturn("TIMESTAMP");
            LocalDateTime expected = LocalDateTime.of(2024, 7, 1, 8, 0);
            Mockito.when(reopened.getObject(1, LocalDateTime.class)).thenReturn(expected);

            Assertions.assertEquals(expected, copy.readTimestamp(reopened, 1));
            Mockito.verify(reopened, Mockito.never()).getTimestamp(1);
        } finally {
            TimeZone.setDefault(original);
        }
    }
}
