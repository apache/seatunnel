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

package org.apache.seatunnel.connectors.seatunnel.natsjetstream.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.config.NatsJetStreamMessageFormat;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.config.NatsJetStreamSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.exception.NatsJetStreamConnectorException;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PublishOptions;
import io.nats.client.impl.Headers;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public class NatsJetStreamSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(NatsJetStreamSinkWriter.class);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration PUBLISH_TIMEOUT = Duration.ofSeconds(30);

    private final int subtaskIndex;
    private final NatsJetStreamRequestSerializer requestSerializer;

    private Connection connection;
    private JetStream jetStream;

    public NatsJetStreamSinkWriter(
            Context context,
            SeaTunnelRowType seaTunnelRowType,
            ReadonlyConfig pluginConfig,
            CatalogTable catalogTable)
            throws IOException {

        NatsJetStreamSinkValidator.validate(pluginConfig, catalogTable);
        this.subtaskIndex = context.getIndexOfSubtask();
        this.requestSerializer = createRequestSerializer(seaTunnelRowType, pluginConfig);
        connect(pluginConfig);
        LOG.info("Opened NATS JetStream sink writer for subtask {}", subtaskIndex);
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        IOException closeException = null;
        if (connection != null) {
            try {
                connection.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                closeException = new IOException("Interrupted while closing NATS connection", e);
            } finally {
                jetStream = null;
                connection = null;
            }
        }
        if (closeException == null) {
            LOG.info("Closed NATS JetStream sink writer for subtask {}", subtaskIndex);
            return;
        }
        throw closeException;
    }

    @Override
    public Optional<Void> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        publish(requestSerializer.serialize(element));
    }

    static NatsJetStreamRequestSerializer createRequestSerializer(
            SeaTunnelRowType seaTunnelRowType, ReadonlyConfig pluginConfig) {
        String defaultSubject =
                normalizeSubject(pluginConfig.getOptional(NatsJetStreamSinkOptions.SUBJECT));
        NatsJetStreamMessageFormat format = pluginConfig.get(NatsJetStreamSinkOptions.FORMAT);
        if (format == NatsJetStreamMessageFormat.JSON) {
            return NatsJetStreamRequestSerializer.forJson(
                    defaultSubject, new JsonSerializationSchema(seaTunnelRowType));
        }
        return NatsJetStreamRequestSerializer.forNative(
                defaultSubject,
                pluginConfig.get(NatsJetStreamSinkOptions.INCLUDE_ROW_KIND_HEADER),
                NativeFieldMapping.of(
                        seaTunnelRowType,
                        pluginConfig.get(NatsJetStreamSinkOptions.NATIVE_FIELDS)));
    }

    static String normalizeSubject(Optional<String> subject) {
        return subject.map(String::trim).filter(value -> !value.isEmpty()).orElse(null);
    }

    private void connect(ReadonlyConfig pluginConfig) throws IOException {
        Options.Builder builder =
                Options.builder()
                        .server(pluginConfig.get(NatsJetStreamSinkOptions.URL))
                        .connectionTimeout(CONNECT_TIMEOUT);

        Optional<String> username = pluginConfig.getOptional(NatsJetStreamSinkOptions.USERNAME);
        Optional<String> password = pluginConfig.getOptional(NatsJetStreamSinkOptions.PASSWORD);
        Optional<String> token = pluginConfig.getOptional(NatsJetStreamSinkOptions.TOKEN);
        boolean hasUsername = username.map(NatsJetStreamSinkWriter::isNotBlank).orElse(false);
        boolean hasPassword = password.map(NatsJetStreamSinkWriter::isNotBlank).orElse(false);
        boolean hasToken = token.map(NatsJetStreamSinkWriter::isNotBlank).orElse(false);
        if (hasUsername && hasPassword) {
            builder.userInfo(username.get().trim(), password.get());
        } else if (hasToken) {
            builder.token(token.get().toCharArray());
        }

        try {
            connection = Nats.connect(builder.build());
            jetStream = connection.jetStream();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while connecting to NATS JetStream", e);
        } catch (IOException | RuntimeException e) {
            IOException failure =
                    new IOException(
                            String.format(
                                    "Failed to connect NATS JetStream sink writer for subtask %d",
                                    subtaskIndex),
                            e);
            closeConnectionQuietly(connection, failure);
            throw failure;
        }
    }

    /**
     * Best-effort close of a connection that could not be fully initialized. Preserves the current
     * thread's interrupt status and attaches any close failure as a suppressed exception on the
     * provided primary failure so the original initialization error remains visible.
     */
    private static void closeConnectionQuietly(Connection toClose, IOException primaryFailure) {
        if (toClose == null) {
            return;
        }
        try {
            toClose.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            primaryFailure.addSuppressed(
                    new IOException("Interrupted while closing NATS connection", e));
        } catch (Exception e) {
            primaryFailure.addSuppressed(new IOException("Failed to close NATS connection", e));
        }
    }

    private void publish(PublishRequest publishRequest) throws IOException {
        PublishOptions.Builder optionsBuilder =
                PublishOptions.builder().streamTimeout(PUBLISH_TIMEOUT);
        if (publishRequest.messageId != null) {
            optionsBuilder.messageId(publishRequest.messageId);
        }
        try {
            jetStream.publish(
                    publishRequest.subject,
                    publishRequest.headers,
                    publishRequest.payload,
                    optionsBuilder.build());
        } catch (JetStreamApiException e) {
            throw publishFailure(publishRequest.subject, e);
        } catch (IOException | RuntimeException e) {
            throw publishFailure(publishRequest.subject, e);
        }
    }

    private IOException publishFailure(String subject, Exception cause) {
        return new IOException(
                String.format(
                        "Failed to publish NATS JetStream message for subtask %d to subject '%s'",
                        subtaskIndex, subject),
                cause);
    }

    private static boolean isNotBlank(String value) {
        return value != null && !value.trim().isEmpty();
    }

    static NatsJetStreamConnectorException invalidRecord(String fieldName, String message) {
        return new NatsJetStreamConnectorException(
                CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                "Invalid NATS JetStream record field `" + fieldName + "`: " + message);
    }

    static final class PublishRequest {
        private final String subject;
        private final String messageId;
        private final Headers headers;
        private final byte[] payload;

        PublishRequest(String subject, String messageId, Headers headers, byte[] payload) {
            this.subject = subject;
            this.messageId = messageId;
            this.headers = headers;
            this.payload = payload;
        }

        String getSubject() {
            return subject;
        }

        String getMessageId() {
            return messageId;
        }

        Headers getHeaders() {
            return headers;
        }

        byte[] getPayload() {
            return payload;
        }
    }

    static final class NativeFieldMapping {
        final int messageIdFieldIndex;
        final int subjectFieldIndex;
        final int headersFieldIndex;
        final int dataFieldIndex;

        private NativeFieldMapping(
                int messageIdFieldIndex,
                int subjectFieldIndex,
                int headersFieldIndex,
                int dataFieldIndex) {
            this.messageIdFieldIndex = messageIdFieldIndex;
            this.subjectFieldIndex = subjectFieldIndex;
            this.headersFieldIndex = headersFieldIndex;
            this.dataFieldIndex = dataFieldIndex;
        }

        static NativeFieldMapping of(SeaTunnelRowType rowType, Map<String, String> nativeFields) {
            return new NativeFieldMapping(
                    resolveFieldIndex(
                            rowType, nativeFields, NatsJetStreamSinkOptions.NATIVE_MAPPING_ID),
                    resolveFieldIndex(
                            rowType, nativeFields, NatsJetStreamSinkOptions.NATIVE_MAPPING_SUBJECT),
                    resolveFieldIndex(
                            rowType, nativeFields, NatsJetStreamSinkOptions.NATIVE_MAPPING_HEADERS),
                    requireFieldIndex(
                            rowType, nativeFields, NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA));
        }

        private static int resolveFieldIndex(
                SeaTunnelRowType rowType, Map<String, String> nativeFields, String mappingKey) {
            String fieldName = nativeFields.get(mappingKey);
            if (fieldName == null || fieldName.trim().isEmpty()) {
                return -1;
            }
            return rowType.indexOf(fieldName, false);
        }

        private static int requireFieldIndex(
                SeaTunnelRowType rowType, Map<String, String> nativeFields, String mappingKey) {
            return rowType.indexOf(nativeFields.get(mappingKey), false);
        }
    }
}
