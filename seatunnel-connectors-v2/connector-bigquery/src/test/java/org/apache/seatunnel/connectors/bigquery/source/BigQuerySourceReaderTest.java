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

package org.apache.seatunnel.connectors.bigquery.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BigQuerySourceReaderTest {

    private static final String TEST_QUERY = "SELECT id, name FROM `proj.ds.tbl`";

    @Test
    public void testPollNextConvertsScalarRows() throws Exception {
        Schema schema =
                Schema.of(
                        Field.of("id", StandardSQLTypeName.INT64),
                        Field.of("name", StandardSQLTypeName.STRING));

        FieldValueList row1 =
                mockRow(
                        mockFieldValue(false, "long", 1L),
                        mockFieldValue(false, "string", "alice"));
        FieldValueList row2 =
                mockRow(mockFieldValue(false, "long", 2L), mockFieldValue(false, "string", "bob"));

        BigQuerySourceReader reader = buildReader(schema, List.of(row1, row2));
        Collector<SeaTunnelRow> collector = mockCollector();

        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector, times(2)).collect(captor.capture());

        List<SeaTunnelRow> rows = captor.getAllValues();
        assertEquals(1L, rows.get(0).getField(0));
        assertEquals("alice", rows.get(0).getField(1));
        assertEquals(2L, rows.get(1).getField(0));
        assertEquals("bob", rows.get(1).getField(1));
    }

    @Test
    public void testNullFieldValueProducesNullInRow() throws Exception {
        Schema schema =
                Schema.of(
                        Field.of("id", StandardSQLTypeName.INT64),
                        Field.of("name", StandardSQLTypeName.STRING));

        FieldValue nullName = mock(FieldValue.class);
        when(nullName.isNull()).thenReturn(true);

        FieldValueList row = mockRow(mockFieldValue(false, "long", 42L), nullName);

        BigQuerySourceReader reader = buildReader(schema, List.of(row));
        Collector<SeaTunnelRow> collector = mockCollector();

        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        SeaTunnelRow collected = captor.getValue();
        assertEquals(42L, collected.getField(0));
        assertNull(collected.getField(1));
    }

    @Test
    public void testRepeatedStringFieldProducesArray() throws Exception {
        Field repeatedField =
                Field.newBuilder("tags", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.REPEATED)
                        .build();
        Schema schema = Schema.of(repeatedField);

        FieldValue elem1 = mock(FieldValue.class);
        when(elem1.getStringValue()).thenReturn("a");
        FieldValue elem2 = mock(FieldValue.class);
        when(elem2.getStringValue()).thenReturn("b");

        FieldValue arrayValue = mock(FieldValue.class);
        when(arrayValue.isNull()).thenReturn(false);
        when(arrayValue.getValue()).thenReturn(List.of(elem1, elem2));

        FieldValueList row = mock(FieldValueList.class);
        when(row.get(0)).thenReturn(arrayValue);

        BigQuerySourceReader reader = buildReader(schema, List.of(row));
        Collector<SeaTunnelRow> collector = mockCollector();

        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        Object[] tags = (Object[]) captor.getValue().getField(0);
        assertArrayEquals(new Object[] {"a", "b"}, tags);
    }

    @Test
    public void testRepeatedFieldWithRawValues() throws Exception {
        Field repeatedField =
                Field.newBuilder("tags", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.REPEATED)
                        .build();
        Schema schema = Schema.of(repeatedField);

        // getValue() returns raw String objects, not FieldValue objects
        FieldValue arrayValue = mock(FieldValue.class);
        when(arrayValue.isNull()).thenReturn(false);
        when(arrayValue.getValue()).thenReturn(List.of("alpha", "beta"));

        FieldValueList row = mock(FieldValueList.class);
        when(row.get(0)).thenReturn(arrayValue);

        BigQuerySourceReader reader = buildReader(schema, List.of(row));
        Collector<SeaTunnelRow> collector = mockCollector();

        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        Object[] tags = (Object[]) captor.getValue().getField(0);
        assertArrayEquals(new Object[] {"alpha", "beta"}, tags);
    }

    @Test
    public void testTimestampConversion() throws Exception {
        Schema schema = Schema.of(Field.of("ts", StandardSQLTypeName.TIMESTAMP));

        // 2021-01-01 00:00:00 UTC = 1609459200_000_000 micros
        long micros = 1609459200_000_000L;
        FieldValue tsValue = mockFieldValue(false, "timestamp", micros);

        FieldValueList row = mockRow(tsValue);

        BigQuerySourceReader reader = buildReader(schema, List.of(row));
        Collector<SeaTunnelRow> collector = mockCollector();

        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        LocalDateTime result = (LocalDateTime) captor.getValue().getField(0);
        assertEquals(LocalDateTime.of(2021, 1, 1, 0, 0, 0), result);
    }

    @Test
    public void testNoMoreSplitSignalsEndOfData() throws Exception {
        BigQuery bigQuery = mock(BigQuery.class);
        SourceReader.Context context = mock(SourceReader.Context.class);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Map.of("project_id", "test-project"));

        BigQuerySourceReader reader = new BigQuerySourceReader(context, config, bigQuery);
        reader.open();
        reader.handleNoMoreSplits();

        Collector<SeaTunnelRow> collector = mockCollector();
        reader.pollNext(collector);

        verify(context).signalNoMoreElement();
    }

    @Test
    public void testQueryFailureThrowsConnectorException() throws Exception {
        Schema schema = Schema.of(Field.of("id", StandardSQLTypeName.INT64));
        BigQuery bigQuery = mock(BigQuery.class);
        when(bigQuery.query(any(QueryJobConfiguration.class)))
                .thenThrow(new InterruptedException("simulated"));

        SourceReader.Context context = mock(SourceReader.Context.class);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Map.of("project_id", "test-project"));

        BigQuerySourceReader reader = new BigQuerySourceReader(context, config, bigQuery);
        reader.open();
        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));

        Collector<SeaTunnelRow> collector = mockCollector();
        assertThrows(BigQueryConnectorException.class, () -> reader.pollNext(collector));
    }

    @Test
    public void testSnapshotStateReturnsPendingSplits() {
        BigQuery bigQuery = mock(BigQuery.class);
        SourceReader.Context context = mock(SourceReader.Context.class);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Map.of("project_id", "test-project"));

        BigQuerySourceReader reader = new BigQuerySourceReader(context, config, bigQuery);
        reader.addSplits(
                List.of(new BigQuerySourceSplit("0", "q1"), new BigQuerySourceSplit("1", "q2")));

        List<BigQuerySourceSplit> snapshot = reader.snapshotState(1L);
        assertEquals(2, snapshot.size());
    }

    @Test
    public void testFetchSizeBatching() throws Exception {
        Schema schema = Schema.of(Field.of("id", StandardSQLTypeName.INT64));
        List<FieldValueList> rows =
                List.of(
                        mockRow(mockFieldValue(false, "long", 1L)),
                        mockRow(mockFieldValue(false, "long", 2L)),
                        mockRow(mockFieldValue(false, "long", 3L)),
                        mockRow(mockFieldValue(false, "long", 4L)),
                        mockRow(mockFieldValue(false, "long", 5L)));

        BigQuery bigQuery = mock(BigQuery.class);
        TableResult tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(rows);
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(tableResult);

        SourceReader.Context context = mock(SourceReader.Context.class);
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("project_id", "test-project");
        configMap.put("fetch_size", 2);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        BigQuerySourceReader reader = new BigQuerySourceReader(context, config, bigQuery);
        reader.open();
        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.handleNoMoreSplits();

        Collector<SeaTunnelRow> collector = mockCollector();

        reader.pollNext(collector);
        verify(collector, times(2)).collect(any(SeaTunnelRow.class));

        reader.pollNext(collector);
        verify(collector, times(4)).collect(any(SeaTunnelRow.class));

        reader.pollNext(collector);
        verify(collector, times(5)).collect(any(SeaTunnelRow.class));

        reader.pollNext(collector);
        verify(context).signalNoMoreElement();
    }

    @Test
    public void testDryRunEmitsNoRows() throws Exception {
        BigQuery bigQuery = mock(BigQuery.class);
        Job mockJob = mock(Job.class);
        JobStatistics.QueryStatistics stats = mock(JobStatistics.QueryStatistics.class);
        when(stats.getEstimatedBytesProcessed()).thenReturn(1234L);
        when(mockJob.getStatistics()).thenReturn(stats);
        when(bigQuery.create(any(JobInfo.class))).thenReturn(mockJob);

        SourceReader.Context context = mock(SourceReader.Context.class);
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("project_id", "test-project");
        configMap.put("dry_run", true);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        BigQuerySourceReader reader = new BigQuerySourceReader(context, config, bigQuery);
        reader.open();
        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));

        Collector<SeaTunnelRow> collector = mockCollector();
        reader.pollNext(collector);

        verify(collector, never()).collect(any(SeaTunnelRow.class));
        verify(context).signalNoMoreElement();
        verify(bigQuery).create(any(JobInfo.class));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testSnapshotStateIncludesCurrentSplitDuringRead() throws Exception {
        Schema schema = Schema.of(Field.of("id", StandardSQLTypeName.INT64));
        List<FieldValueList> rows =
                List.of(
                        mockRow(mockFieldValue(false, "long", 1L)),
                        mockRow(mockFieldValue(false, "long", 2L)),
                        mockRow(mockFieldValue(false, "long", 3L)),
                        mockRow(mockFieldValue(false, "long", 4L)),
                        mockRow(mockFieldValue(false, "long", 5L)));

        BigQuery bigQuery = mock(BigQuery.class);
        TableResult tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(rows);
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(tableResult);

        SourceReader.Context context = mock(SourceReader.Context.class);
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("project_id", "test-project");
        configMap.put("fetch_size", 2);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        BigQuerySourceReader reader = new BigQuerySourceReader(context, config, bigQuery);
        reader.open();
        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.handleNoMoreSplits();

        Collector<SeaTunnelRow> collector = mockCollector();

        reader.pollNext(collector);
        List<BigQuerySourceSplit> snapshot = reader.snapshotState(1L);
        assertEquals(
                1, snapshot.size(), "Snapshot should include the split being processed mid-read");

        reader.pollNext(collector);
        snapshot = reader.snapshotState(2L);
        assertEquals(1, snapshot.size());

        reader.pollNext(collector);
        snapshot = reader.snapshotState(3L);
        assertEquals(0, snapshot.size(), "Snapshot should be empty after split is fully consumed");
    }

    @Test
    public void testBoolConversion() throws Exception {
        Schema schema = Schema.of(Field.of("active", StandardSQLTypeName.BOOL));
        FieldValueList row = mockRow(mockFieldValue(false, "boolean", true));

        BigQuerySourceReader reader = buildReader(schema, List.of(row));
        Collector<SeaTunnelRow> collector = mockCollector();
        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        assertEquals(true, captor.getValue().getField(0));
    }

    @Test
    public void testFloat64Conversion() throws Exception {
        Schema schema = Schema.of(Field.of("score", StandardSQLTypeName.FLOAT64));
        FieldValueList row = mockRow(mockFieldValue(false, "double", 3.14));

        BigQuerySourceReader reader = buildReader(schema, List.of(row));
        Collector<SeaTunnelRow> collector = mockCollector();
        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        assertEquals(3.14, captor.getValue().getField(0));
    }

    @Test
    public void testNumericConversion() throws Exception {
        Schema schema = Schema.of(Field.of("price", StandardSQLTypeName.NUMERIC));
        BigDecimal expected = new BigDecimal("99.99");
        FieldValueList row = mockRow(mockFieldValue(false, "numeric", expected));

        BigQuerySourceReader reader = buildReader(schema, List.of(row));
        Collector<SeaTunnelRow> collector = mockCollector();
        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        assertEquals(expected, captor.getValue().getField(0));
    }

    @Test
    public void testBytesConversion() throws Exception {
        Schema schema = Schema.of(Field.of("data", StandardSQLTypeName.BYTES));
        byte[] expected = "hello".getBytes();
        String encoded = Base64.getEncoder().encodeToString(expected);
        FieldValueList row = mockRow(mockFieldValue(false, "string", encoded));

        BigQuerySourceReader reader = buildReader(schema, List.of(row));
        Collector<SeaTunnelRow> collector = mockCollector();
        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        assertArrayEquals(expected, (byte[]) captor.getValue().getField(0));
    }

    @Test
    public void testDateConversion() throws Exception {
        Schema schema = Schema.of(Field.of("birth_date", StandardSQLTypeName.DATE));
        FieldValueList row = mockRow(mockFieldValue(false, "string", "2026-01-15"));

        BigQuerySourceReader reader = buildReader(schema, List.of(row));
        Collector<SeaTunnelRow> collector = mockCollector();
        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        assertEquals(LocalDate.of(2026, 1, 15), captor.getValue().getField(0));
    }

    @Test
    public void testTimeConversion() throws Exception {
        Schema schema = Schema.of(Field.of("start_time", StandardSQLTypeName.TIME));
        FieldValueList row = mockRow(mockFieldValue(false, "string", "12:30:45"));

        BigQuerySourceReader reader = buildReader(schema, List.of(row));
        Collector<SeaTunnelRow> collector = mockCollector();
        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        assertEquals(LocalTime.of(12, 30, 45), captor.getValue().getField(0));
    }

    @Test
    public void testDatetimeConversion() throws Exception {
        Schema schema = Schema.of(Field.of("event_ts", StandardSQLTypeName.DATETIME));
        FieldValueList row = mockRow(mockFieldValue(false, "string", "2026-01-15T12:30:45"));

        BigQuerySourceReader reader = buildReader(schema, List.of(row));
        Collector<SeaTunnelRow> collector = mockCollector();
        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        assertEquals(LocalDateTime.of(2026, 1, 15, 12, 30, 45), captor.getValue().getField(0));
    }

    @Test
    public void testStructFieldProducesNestedRow() throws Exception {
        Field structField =
                Field.newBuilder(
                                "address",
                                StandardSQLTypeName.STRUCT,
                                Field.of("street", StandardSQLTypeName.STRING),
                                Field.of("city", StandardSQLTypeName.STRING))
                        .build();
        Schema schema = Schema.of(structField);

        FieldValueList recordValue = mock(FieldValueList.class);
        FieldValue streetValue = mockFieldValue(false, "string", "123 Main St");
        FieldValue cityValue = mockFieldValue(false, "string", "NYC");
        when(recordValue.get(0)).thenReturn(streetValue);
        when(recordValue.get(1)).thenReturn(cityValue);

        FieldValue structValue = mock(FieldValue.class);
        when(structValue.isNull()).thenReturn(false);
        when(structValue.getRecordValue()).thenReturn(recordValue);

        FieldValueList row = mock(FieldValueList.class);
        when(row.get(0)).thenReturn(structValue);

        BigQuerySourceReader reader = buildReader(schema, List.of(row));
        Collector<SeaTunnelRow> collector = mockCollector();
        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));
        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        SeaTunnelRow nested = (SeaTunnelRow) captor.getValue().getField(0);
        assertEquals("123 Main St", nested.getField(0));
        assertEquals("NYC", nested.getField(1));
    }

    @Test
    public void testNullFieldTypeThrowsException() throws Exception {
        Field nullTypeField = mock(Field.class);
        when(nullTypeField.getName()).thenReturn("bad_field");
        when(nullTypeField.getMode()).thenReturn(Field.Mode.NULLABLE);
        when(nullTypeField.getType()).thenReturn(null);

        Schema schema = Schema.of(nullTypeField);

        FieldValue nonNullValue = mock(FieldValue.class);
        when(nonNullValue.isNull()).thenReturn(false);
        FieldValueList row = mockRow(nonNullValue);

        BigQuery bigQuery = mock(BigQuery.class);
        TableResult tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(List.of(row));
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(tableResult);

        SourceReader.Context context = mock(SourceReader.Context.class);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Map.of("project_id", "test-project"));

        BigQuerySourceReader reader = new BigQuerySourceReader(context, config, bigQuery);
        reader.open();
        reader.addSplits(List.of(new BigQuerySourceSplit("0", TEST_QUERY)));

        Collector<SeaTunnelRow> collector = mockCollector();
        assertThrows(BigQueryConnectorException.class, () -> reader.pollNext(collector));
    }

    // ---- helpers ----

    private BigQuerySourceReader buildReader(Schema schema, List<FieldValueList> rows)
            throws Exception {
        BigQuery bigQuery = mock(BigQuery.class);
        TableResult tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(rows);
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(tableResult);

        SourceReader.Context context = mock(SourceReader.Context.class);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Map.of("project_id", "test-project"));

        BigQuerySourceReader reader = new BigQuerySourceReader(context, config, bigQuery);
        reader.open();
        return reader;
    }

    @SuppressWarnings("unchecked")
    private Collector<SeaTunnelRow> mockCollector() {
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());
        return collector;
    }

    private FieldValueList mockRow(FieldValue... values) {
        FieldValueList row = mock(FieldValueList.class);
        for (int i = 0; i < values.length; i++) {
            when(row.get(i)).thenReturn(values[i]);
        }
        return row;
    }

    private FieldValue mockFieldValue(boolean isNull, String accessor, Object returnValue) {
        FieldValue fv = mock(FieldValue.class);
        when(fv.isNull()).thenReturn(isNull);
        if (!isNull) {
            switch (accessor) {
                case "long":
                    when(fv.getLongValue()).thenReturn((Long) returnValue);
                    break;
                case "string":
                    when(fv.getStringValue()).thenReturn((String) returnValue);
                    break;
                case "boolean":
                    when(fv.getBooleanValue()).thenReturn((Boolean) returnValue);
                    break;
                case "double":
                    when(fv.getDoubleValue()).thenReturn((Double) returnValue);
                    break;
                case "numeric":
                    when(fv.getNumericValue()).thenReturn((BigDecimal) returnValue);
                    break;
                case "timestamp":
                    when(fv.getTimestampValue()).thenReturn((Long) returnValue);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown accessor: " + accessor);
            }
        }
        return fv;
    }
}
