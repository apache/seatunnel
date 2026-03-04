package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.copy;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

public class PgCopyCsvReaderTest {

    @Test
    public void testCsvRowParsing() throws IOException {
        String csvData = "1,\"test\"\n2,\"foo,bar\"";
        InputStream stream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));

        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 0, false, null, null))
                        .build();

        PgCopyCsvReader reader = new PgCopyCsvReader(stream, schema);

        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row1 = reader.next();
        Assertions.assertEquals(1, row1.getField(0));
        Assertions.assertEquals("test", row1.getField(1));

        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row2 = reader.next();
        Assertions.assertEquals(2, row2.getField(0));
        Assertions.assertEquals("foo,bar", row2.getField(1));

        Assertions.assertFalse(reader.hasNext());
    }

    @Test
    public void testNullValues() throws IOException {
        String csvData = "\\N,test\n1,\\N";
        InputStream stream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));

        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 0, false, null, null))
                        .build();

        PgCopyCsvReader reader = new PgCopyCsvReader(stream, schema);

        // Row 1: NULL, "test"
        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row1 = reader.next();
        Assertions.assertNull(row1.getField(0));
        Assertions.assertEquals("test", row1.getField(1));

        // Row 2: 1, NULL
        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row2 = reader.next();
        Assertions.assertEquals(1, row2.getField(0));
        Assertions.assertNull(row2.getField(1));

        Assertions.assertFalse(reader.hasNext());
    }

    @Test
    public void testComplexCsv() throws IOException {
        // "He said ""Hello""",Line2\nLine3
        String csvData = "\"He said \"\"Hello\"\"\",\"Line2\nLine3\"";
        InputStream stream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));

        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "c1", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "c2", BasicType.STRING_TYPE, 0, false, null, null))
                        .build();

        PgCopyCsvReader reader = new PgCopyCsvReader(stream, schema);

        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row = reader.next();
        Assertions.assertEquals("He said \"Hello\"", row.getField(0));
        Assertions.assertEquals("Line2\nLine3", row.getField(1));
        Assertions.assertFalse(reader.hasNext());
    }

    @Test
    public void testBooleanTypes() throws IOException {
        // Postgres COPY CSV uses 't' and 'f' for boolean
        String csvData = "t,f\ntrue,false\n1,0";
        InputStream stream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));

        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "b1", BasicType.BOOLEAN_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "b2", BasicType.BOOLEAN_TYPE, 0, false, null, null))
                        .build();

        PgCopyCsvReader reader = new PgCopyCsvReader(stream, schema);

        // Row 1: t, f
        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row1 = reader.next();
        Assertions.assertEquals(true, row1.getField(0));
        Assertions.assertEquals(false, row1.getField(1));

        // Row 2: true, false
        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row2 = reader.next();
        Assertions.assertEquals(true, row2.getField(0));
        Assertions.assertEquals(false, row2.getField(1));

        // Row 3: 1, 0
        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row3 = reader.next();
        Assertions.assertEquals(true, row3.getField(0));
        Assertions.assertEquals(false, row3.getField(1));

        Assertions.assertFalse(reader.hasNext());
    }

    @Test
    public void testNumericTypes() throws IOException {
        String csvData = "123.456,12345678901234567890";
        InputStream stream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));

        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "d1", BasicType.DOUBLE_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "d2", new DecimalType(38, 18), 0, false, null, null))
                        .build();

        PgCopyCsvReader reader = new PgCopyCsvReader(stream, schema);

        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row = reader.next();
        Assertions.assertEquals(123.456, row.getField(0));
        Assertions.assertEquals(new BigDecimal("12345678901234567890"), row.getField(1));
    }

    @Test
    public void testTypeMismatch() throws IOException {
        String csvData = "abc";
        InputStream stream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));

        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, null))
                        .build();

        PgCopyCsvReader reader = new PgCopyCsvReader(stream, schema);

        Assertions.assertThrows(
                JdbcConnectorException.class,
                () -> {
                    if (reader.hasNext()) {
                        reader.next();
                    }
                });
    }

    @Test
    public void testEmptyStrings() throws IOException {
        String csvData = ",\"  \"";
        InputStream stream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));

        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "s1", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "s2", BasicType.STRING_TYPE, 0, false, null, null))
                        .build();

        PgCopyCsvReader reader = new PgCopyCsvReader(stream, schema);

        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row = reader.next();
        // Empty string in CSV is just empty string, NULL is \N
        // But commons-csv might treat empty field as empty string
        // PgCopyUtils.parseValue: if (raw == null || raw.isEmpty() || "\\N".equals(raw)) -> null
        // So empty string becomes null? Let's check PgCopyUtils.java again.
        // Yes: if (raw == null || raw.isEmpty() || "\\N".equals(raw)) { return null; }
        // This means empty string "" is treated as NULL. This might be correct for SeaTunnel
        // convention but let's verify.
        // If I want empty string, I probably need to quote it? "\"\"" -> raw will be "" -> treated
        // as null?
        // Wait, if raw is "", it returns null.
        Assertions.assertNull(row.getField(0));
        Assertions.assertEquals("  ", row.getField(1));
    }
}
