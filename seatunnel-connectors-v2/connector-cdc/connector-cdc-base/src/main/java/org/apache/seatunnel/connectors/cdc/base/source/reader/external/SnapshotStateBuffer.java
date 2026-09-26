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

package org.apache.seatunnel.connectors.cdc.base.source.reader.external;

import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/** A temporary disk-backed keyed buffer used while reconciling snapshot and binlog records. */
public final class SnapshotStateBuffer implements AutoCloseable {

    private static final String TOPIC = "snapshot-state-buffer";
    private static final byte[] NULL_KEY = new byte[] {0};
    private static final int COMMIT_INTERVAL = 1_000;
    private static final int SQLITE_CACHE_KILOBYTES = 8 * 1_024;

    private final Path directory;
    private final Connection connection;
    private final JsonConverter converter;
    private final PreparedStatement upsertStatement;
    private final PreparedStatement removeStatement;

    private long nextOrdinal;
    private int pendingWrites;
    private SnapshotIterator activeIterator;
    private volatile boolean closed;

    public static SnapshotStateBuffer create() throws IOException {
        return create(null);
    }

    public static SnapshotStateBuffer create(Path parentDirectory) throws IOException {
        Path directory =
                parentDirectory == null
                        ? Files.createTempDirectory("seatunnel-cdc-snapshot-")
                        : Files.createTempDirectory(parentDirectory, "seatunnel-cdc-snapshot-");
        try {
            Class.forName("org.sqlite.JDBC", true, SnapshotStateBuffer.class.getClassLoader());
            return new SnapshotStateBuffer(directory);
        } catch (Exception e) {
            deleteDirectory(directory);
            throw new IOException("Unable to create the CDC snapshot state buffer", e);
        }
    }

    private SnapshotStateBuffer(Path directory) throws SQLException {
        this.directory = directory;
        this.converter = new JsonConverter();
        this.converter.configure(Collections.singletonMap("schemas.enable", true), false);

        Connection openedConnection =
                DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("state.db"));
        PreparedStatement openedUpsertStatement = null;
        PreparedStatement openedRemoveStatement = null;
        try {
            openedConnection.setAutoCommit(false);
            try (Statement statement = openedConnection.createStatement()) {
                statement.execute("PRAGMA cache_size=-" + SQLITE_CACHE_KILOBYTES);
                statement.execute("PRAGMA temp_store=FILE");
                statement.execute(
                        "CREATE TABLE snapshot_state ("
                                + "record_key BLOB PRIMARY KEY, ordinal INTEGER NOT NULL, payload BLOB NOT NULL)");
                openedConnection.commit();
            }
            openedUpsertStatement =
                    openedConnection.prepareStatement(
                            "INSERT INTO snapshot_state(record_key, ordinal, payload) VALUES (?, ?, ?) "
                                    + "ON CONFLICT(record_key) DO UPDATE SET payload=excluded.payload");
            openedRemoveStatement =
                    openedConnection.prepareStatement(
                            "DELETE FROM snapshot_state WHERE record_key = ?");
        } catch (SQLException e) {
            closeQuietly(openedRemoveStatement);
            closeQuietly(openedUpsertStatement);
            closeQuietly(openedConnection);
            throw e;
        }
        this.connection = openedConnection;
        this.upsertStatement = openedUpsertStatement;
        this.removeStatement = openedRemoveStatement;
    }

    public void put(SourceRecord record) {
        ensureOpen();
        try {
            upsertStatement.setBytes(1, serializeKey(record));
            upsertStatement.setLong(2, nextOrdinal++);
            upsertStatement.setBytes(3, serializeRecord(record));
            upsertStatement.executeUpdate();
            commitPeriodically();
        } catch (Exception e) {
            throw new SeaTunnelException(
                    "Failed to buffer snapshot record for topic " + record.topic(), e);
        }
    }

    public void remove(org.apache.kafka.connect.data.Struct key) {
        ensureOpen();
        try {
            removeStatement.setBytes(
                    1,
                    key == null ? NULL_KEY : converter.fromConnectData(TOPIC, key.schema(), key));
            removeStatement.executeUpdate();
            commitPeriodically();
        } catch (Exception e) {
            throw new SeaTunnelException("Failed to remove a CDC snapshot record", e);
        }
    }

    public Iterator<SourceRecord> iterator() {
        ensureOpen();
        if (activeIterator != null) {
            throw new IllegalStateException(
                    "The snapshot state buffer supports one output iterator");
        }
        try {
            connection.commit();
            Statement statement = connection.createStatement();
            ResultSet resultSet =
                    statement.executeQuery("SELECT payload FROM snapshot_state ORDER BY ordinal");
            activeIterator = new SnapshotIterator(statement, resultSet);
            return activeIterator;
        } catch (SQLException e) {
            throw new SeaTunnelException("Failed to read buffered CDC snapshot records", e);
        }
    }

    private byte[] serializeKey(SourceRecord record) {
        if (record.key() == null) {
            return NULL_KEY;
        }
        return converter.fromConnectData(TOPIC, record.keySchema(), record.key());
    }

    private byte[] serializeRecord(SourceRecord record) throws IOException {
        byte[] value = converter.fromConnectData(TOPIC, record.valueSchema(), record.value());
        List<HeaderData> headers = new ArrayList<>();
        for (Header header : record.headers()) {
            headers.add(
                    new HeaderData(
                            header.key(),
                            converter.fromConnectData(TOPIC, header.schema(), header.value())));
        }

        ByteArrayOutputStream metadataBytes = new ByteArrayOutputStream();
        try (ObjectOutputStream metadataOutput = new ObjectOutputStream(metadataBytes)) {
            metadataOutput.writeObject(
                    new RecordMetadata(
                            new LinkedHashMap<>(record.sourcePartition()),
                            new LinkedHashMap<>(record.sourceOffset()),
                            record.topic(),
                            record.kafkaPartition(),
                            record.timestamp(),
                            headers));
        }

        ByteArrayOutputStream recordBytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(recordBytes)) {
            writeBytes(output, converter.fromConnectData(TOPIC, record.keySchema(), record.key()));
            writeBytes(output, value);
            writeBytes(output, metadataBytes.toByteArray());
        }
        return recordBytes.toByteArray();
    }

    private SourceRecord deserializeRecord(byte[] payload)
            throws IOException, ClassNotFoundException {
        byte[] key;
        byte[] value;
        byte[] metadataBytes;
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            key = readBytes(input);
            value = readBytes(input);
            metadataBytes = readBytes(input);
        }

        RecordMetadata metadata;
        try (ObjectInputStream metadataInput =
                new ObjectInputStream(new ByteArrayInputStream(metadataBytes))) {
            metadata = (RecordMetadata) metadataInput.readObject();
        }

        SchemaAndValue keyData =
                key == null ? new SchemaAndValue(null, null) : converter.toConnectData(TOPIC, key);
        SchemaAndValue valueData = converter.toConnectData(TOPIC, value);
        ConnectHeaders headers = new ConnectHeaders();
        for (HeaderData header : metadata.headers) {
            SchemaAndValue headerData = converter.toConnectData(TOPIC, header.value);
            headers.add(header.key, headerData.value(), headerData.schema());
        }
        return new SourceRecord(
                metadata.sourcePartition,
                metadata.sourceOffset,
                metadata.topic,
                metadata.kafkaPartition,
                keyData.schema(),
                keyData.value(),
                valueData.schema(),
                valueData.value(),
                metadata.timestamp,
                headers);
    }

    private static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
        if (value == null) {
            output.writeInt(-1);
            return;
        }
        output.writeInt(value.length);
        output.write(value);
    }

    private static byte[] readBytes(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 0) {
            return null;
        }
        byte[] value = new byte[length];
        input.readFully(value);
        return value;
    }

    private void commitPeriodically() throws SQLException {
        if (++pendingWrites >= COMMIT_INTERVAL) {
            connection.commit();
            pendingWrites = 0;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("The CDC snapshot state buffer is closed");
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeQuietly(activeIterator);
        closeQuietly(upsertStatement);
        closeQuietly(removeStatement);
        closeQuietly(connection);
        deleteDirectory(directory);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Temporary-buffer cleanup is best effort after a split has completed or failed.
        }
    }

    private static void deleteDirectory(Path directory) {
        if (directory == null || !Files.exists(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            paths.sorted((left, right) -> right.compareTo(left))
                    .forEach(
                            path -> {
                                try {
                                    Files.deleteIfExists(path);
                                } catch (IOException ignored) {
                                    // The directory is private to this buffer and may be removed by
                                    // the operating system's temporary-file cleanup later.
                                }
                            });
        } catch (IOException ignored) {
            // The directory is private to this buffer and may be removed by the operating system.
        }
    }

    private final class SnapshotIterator implements Iterator<SourceRecord>, AutoCloseable {
        private final Statement statement;
        private final ResultSet resultSet;
        private SourceRecord nextRecord;
        private boolean iteratorClosed;

        private SnapshotIterator(Statement statement, ResultSet resultSet) {
            this.statement = statement;
            this.resultSet = resultSet;
        }

        @Override
        public boolean hasNext() {
            if (iteratorClosed) {
                return false;
            }
            if (nextRecord != null) {
                return true;
            }
            try {
                if (!resultSet.next()) {
                    close();
                    return false;
                }
                nextRecord = deserializeRecord(resultSet.getBytes(1));
                return true;
            } catch (Exception e) {
                close();
                throw new SeaTunnelException("Failed to decode a buffered CDC snapshot record", e);
            }
        }

        @Override
        public SourceRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            SourceRecord record = nextRecord;
            nextRecord = null;
            return record;
        }

        @Override
        public void close() {
            if (iteratorClosed) {
                return;
            }
            iteratorClosed = true;
            closeQuietly(resultSet);
            closeQuietly(statement);
        }
    }

    private static final class RecordMetadata implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private final Map<String, ?> sourcePartition;
        private final Map<String, ?> sourceOffset;
        private final String topic;
        private final Integer kafkaPartition;
        private final Long timestamp;
        private final List<HeaderData> headers;

        private RecordMetadata(
                Map<String, ?> sourcePartition,
                Map<String, ?> sourceOffset,
                String topic,
                Integer kafkaPartition,
                Long timestamp,
                List<HeaderData> headers) {
            this.sourcePartition = sourcePartition;
            this.sourceOffset = sourceOffset;
            this.topic = topic;
            this.kafkaPartition = kafkaPartition;
            this.timestamp = timestamp;
            this.headers = headers;
        }
    }

    private static final class HeaderData implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private final String key;
        private final byte[] value;

        private HeaderData(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }
}
