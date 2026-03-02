package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.copy;

import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class PgCopyBinaryReaderTest {

    private static final byte[] SIGNATURE = {
        'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xFF, '\r', '\n', 0
    };

    private static final LocalDate PG_EPOCH_DATE = LocalDate.of(2000, 1, 1);
    private static final LocalDateTime PG_EPOCH_DATETIME = LocalDateTime.of(2000, 1, 1, 0, 0);

    @Test
    public void testBinaryHeaderParsing() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(SIGNATURE);
        baos.write(new byte[4]); // flags
        baos.write(new byte[4]); // extension length
        // Add EOF marker to ensure clean termination
        baos.write(new byte[] {(byte) 0xFF, (byte) 0xFF});

        InputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TableSchema schema =
                TableSchema.builder()
                        .addColumn(
                                PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, null))
                        .build();

        PgCopyBinaryReader reader = new PgCopyBinaryReader(stream, schema, 65536);
        // Header parsing happens lazily. Since we only provided header + EOF, hasNext should be
        // false
        Assertions.assertFalse(reader.hasNext());
    }

    @Test
    public void testInvalidSignature() throws IOException {
        byte[] invalidSig = "INVALID_SIG".getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream stream = new ByteArrayInputStream(invalidSig);
        TableSchema schema =
                TableSchema.builder()
                        .addColumn(
                                PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, null))
                        .build();

        PgCopyBinaryReader reader = new PgCopyBinaryReader(stream, schema, 65536);
        Assertions.assertThrows(
                JdbcConnectorException.class,
                () -> {
                    // Trigger header parsing
                    reader.hasNext();
                    reader.next();
                });
    }

    @Test
    public void testRowParsing() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Header
        dos.write(SIGNATURE);
        dos.writeInt(0); // flags
        dos.writeInt(0); // extension length

        // Row 1: 1, "test"
        dos.writeShort(2); // field count

        // Field 1: INT 1
        dos.writeInt(4); // length
        dos.writeInt(1); // value

        // Field 2: STRING "test"
        byte[] strBytes = "test".getBytes(StandardCharsets.UTF_8);
        dos.writeInt(strBytes.length); // length
        dos.write(strBytes); // value

        // Trailer
        dos.writeShort(-1); // EOF

        InputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TableSchema schema =
                TableSchema.builder()
                        .addColumn(
                                PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, null))
                        .addColumn(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 0, false, null, null))
                        .build();

        PgCopyBinaryReader reader = new PgCopyBinaryReader(stream, schema, 65536);

        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row = reader.next();
        Assertions.assertNotNull(row);
        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertEquals("test", row.getField(1));

        Assertions.assertFalse(reader.hasNext());
    }

    @Test
    public void testNullValues() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Header
        dos.write(SIGNATURE);
        dos.writeInt(0);
        dos.writeInt(0);

        // Row 1: NULL, NULL
        dos.writeShort(2); // field count
        dos.writeInt(-1); // Field 1: NULL (length -1)
        dos.writeInt(-1); // Field 2: NULL (length -1)

        // Trailer
        dos.writeShort(-1);

        InputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TableSchema schema =
                TableSchema.builder()
                        .addColumn(
                                PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, null))
                        .addColumn(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 0, false, null, null))
                        .build();

        PgCopyBinaryReader reader = new PgCopyBinaryReader(stream, schema, 65536);

        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row = reader.next();
        Assertions.assertNull(row.getField(0));
        Assertions.assertNull(row.getField(1));
        Assertions.assertFalse(reader.hasNext());
    }

    @Test
    public void testEmptyString() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Header
        dos.write(SIGNATURE);
        dos.writeInt(0);
        dos.writeInt(0);

        // Row 1: "" (Empty String)
        dos.writeShort(1);
        dos.writeInt(0); // length 0

        // Trailer
        dos.writeShort(-1);

        InputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TableSchema schema =
                TableSchema.builder()
                        .addColumn(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 0, false, null, null))
                        .build();

        PgCopyBinaryReader reader = new PgCopyBinaryReader(stream, schema, 65536);

        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row = reader.next();
        Assertions.assertEquals("", row.getField(0));
        Assertions.assertFalse(reader.hasNext());
    }

    @Test
    public void testColumnCountMismatch() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Header
        dos.write(SIGNATURE);
        dos.writeInt(0);
        dos.writeInt(0);

        // Row 1: Only 1 field, but schema expects 2
        dos.writeShort(1); // field count = 1
        dos.writeInt(4);
        dos.writeInt(100);

        // Trailer
        dos.writeShort(-1);

        InputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TableSchema schema =
                TableSchema.builder()
                        .addColumn(
                                PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, null))
                        .addColumn(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 0, false, null, null))
                        .build();

        PgCopyBinaryReader reader = new PgCopyBinaryReader(stream, schema, 65536);

        Assertions.assertThrows(
                JdbcConnectorException.class,
                () -> {
                    if (reader.hasNext()) {
                        reader.next();
                    }
                });
    }

    @Test
    public void testMoreDataTypes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Header
        dos.write(SIGNATURE);
        dos.writeInt(0);
        dos.writeInt(0);

        // Row 1: BIGINT, BOOLEAN, DOUBLE
        dos.writeShort(3);

        // Field 1: BIGINT 1234567890L
        dos.writeInt(8);
        dos.writeLong(1234567890L);

        // Field 2: BOOLEAN true (1 byte)
        dos.writeInt(1);
        dos.writeByte(1);

        // Field 3: DOUBLE 3.14159
        dos.writeInt(8);
        dos.writeDouble(3.14159);

        // Trailer
        dos.writeShort(-1);

        InputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TableSchema schema =
                TableSchema.builder()
                        .addColumn(
                                PhysicalColumn.of(
                                        "c_bigint", BasicType.LONG_TYPE, 0, false, null, null))
                        .addColumn(
                                PhysicalColumn.of(
                                        "c_bool", BasicType.BOOLEAN_TYPE, 0, false, null, null))
                        .addColumn(
                                PhysicalColumn.of(
                                        "c_double", BasicType.DOUBLE_TYPE, 0, false, null, null))
                        .build();

        PgCopyBinaryReader reader = new PgCopyBinaryReader(stream, schema, 65536);

        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row = reader.next();
        Assertions.assertEquals(1234567890L, row.getField(0));
        Assertions.assertEquals(true, row.getField(1));
        Assertions.assertEquals(3.14159, row.getField(2));
        Assertions.assertFalse(reader.hasNext());
    }

    @Test
    public void testDateAndTimestamp() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Header
        dos.write(SIGNATURE);
        dos.writeInt(0);
        dos.writeInt(0);

        // Row 1: DATE, TIMESTAMP
        dos.writeShort(2);

        LocalDate testDate = LocalDate.of(2023, 10, 1);
        int daysDiff = (int) ChronoUnit.DAYS.between(PG_EPOCH_DATE, testDate);
        dos.writeInt(4);
        dos.writeInt(daysDiff);

        LocalDateTime testDateTime = LocalDateTime.of(2023, 10, 1, 12, 30, 45);
        long microsDiff = ChronoUnit.MICROS.between(PG_EPOCH_DATETIME, testDateTime);
        dos.writeInt(8);
        dos.writeLong(microsDiff);

        // Trailer
        dos.writeShort(-1);

        InputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TableSchema schema =
                TableSchema.builder()
                        .addColumn(
                                PhysicalColumn.of(
                                        "c_date",
                                        LocalTimeType.LOCAL_DATE_TYPE,
                                        0,
                                        false,
                                        null,
                                        null))
                        .addColumn(
                                PhysicalColumn.of(
                                        "c_timestamp",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        0,
                                        false,
                                        null,
                                        null))
                        .build();

        PgCopyBinaryReader reader = new PgCopyBinaryReader(stream, schema, 65536);

        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row = reader.next();
        Assertions.assertEquals(testDate, row.getField(0));
        Assertions.assertEquals(testDateTime, row.getField(1));
        Assertions.assertFalse(reader.hasNext());
    }

    @Test
    public void testByteAAndSmallIntFloat() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Header
        dos.write(SIGNATURE);
        dos.writeInt(0);
        dos.writeInt(0);

        // Row 1: BYTEA, SMALLINT, FLOAT(Real)
        dos.writeShort(3);

        // BYTEA
        byte[] bytes = new byte[] {1, 2, 3, 4};
        dos.writeInt(bytes.length);
        dos.write(bytes);

        // SMALLINT (Short)
        dos.writeInt(2);
        dos.writeShort(32000);

        // FLOAT (Real)
        dos.writeInt(4);
        dos.writeFloat(1.23f);

        // Trailer
        dos.writeShort(-1);

        InputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TableSchema schema =
                TableSchema.builder()
                        .addColumn(
                                PhysicalColumn.of(
                                        "c_bytea",
                                        PrimitiveByteArrayType.INSTANCE,
                                        0,
                                        false,
                                        null,
                                        null))
                        .addColumn(
                                PhysicalColumn.of(
                                        "c_short", BasicType.SHORT_TYPE, 0, false, null, null))
                        .addColumn(
                                PhysicalColumn.of(
                                        "c_float", BasicType.FLOAT_TYPE, 0, false, null, null))
                        .build();

        PgCopyBinaryReader reader = new PgCopyBinaryReader(stream, schema, 65536);

        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row = reader.next();
        Assertions.assertArrayEquals(bytes, (byte[]) row.getField(0));
        Assertions.assertEquals((short) 32000, row.getField(1));
        Assertions.assertEquals(1.23f, row.getField(2));
        Assertions.assertFalse(reader.hasNext());
    }

    @Test
    public void testPrematureEof() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Header
        dos.write(SIGNATURE);
        dos.writeInt(0);
        dos.writeInt(0);

        // Row start but incomplete
        dos.writeShort(2);
        dos.writeInt(4);
        // Missing value data

        InputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TableSchema schema =
                TableSchema.builder()
                        .addColumn(
                                PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, null))
                        .addColumn(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 0, false, null, null))
                        .build();

        PgCopyBinaryReader reader = new PgCopyBinaryReader(stream, schema, 65536);

        Assertions.assertThrows(
                JdbcConnectorException.class,
                () -> {
                    if (reader.hasNext()) {
                        reader.next();
                    }
                });
    }

    @Test
    public void testBufferExpansion() throws IOException {
        // Use a small buffer size to trigger expansion
        int smallBufferSize = 128;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Header
        dos.write(SIGNATURE);
        dos.writeInt(0);
        dos.writeInt(0);

        // Row with large field (> 128 bytes)
        dos.writeShort(1);

        byte[] largeData = new byte[300]; // Larger than smallBufferSize
        for (int i = 0; i < 300; i++) largeData[i] = (byte) (i % 255);

        dos.writeInt(largeData.length);
        dos.write(largeData);

        // Trailer
        dos.writeShort(-1);

        InputStream stream = new ByteArrayInputStream(baos.toByteArray());
        TableSchema schema =
                TableSchema.builder()
                        .addColumn(
                                PhysicalColumn.of(
                                        "data",
                                        PrimitiveByteArrayType.INSTANCE,
                                        0,
                                        false,
                                        null,
                                        null))
                        .build();

        // Initialize with small buffer size (will be rounded up to power of 2, 128)
        PgCopyBinaryReader reader = new PgCopyBinaryReader(stream, schema, smallBufferSize);

        Assertions.assertTrue(reader.hasNext());
        SeaTunnelRow row = reader.next();
        Assertions.assertArrayEquals(largeData, (byte[]) row.getField(0));
        Assertions.assertFalse(reader.hasNext());
    }
}
