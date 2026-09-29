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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.payload;

import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorException;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.EdgeSocketQueuedRecord;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketCompressionType;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketPayloadDeserializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

public class EdgeSocketCompressionPayloadDeserializer implements EdgeSocketPayloadDeserializer {

    /**
     * Decode queued payload bytes and return UTF-8 text.
     *
     * @param queuedRecord queued payload with compression metadata
     * @return decompressed plain text payload
     */
    @Override
    public String deserializeRecord(EdgeSocketQueuedRecord queuedRecord) {
        byte[] plainBytes =
                decompress(queuedRecord.getPayloadBytes(), queuedRecord.getCompressionType());
        return new String(plainBytes, StandardCharsets.UTF_8);
    }

    /**
     * Dispatch payload decompression by compression type.
     *
     * @param input payload bytes before decompression
     * @param compressionType compression algorithm declared by packet
     * @return decompressed bytes
     */
    private byte[] decompress(byte[] input, EdgeSocketCompressionType compressionType) {
        switch (compressionType) {
            case NONE:
                return input;
            case GZIP:
                return readAll(new GZIPInputStreamWrapper(input));
            case ZLIB:
                return readAll(new InflaterInputStreamWrapper(input, new Inflater()));
            case DEFLATE:
                return readAll(new InflaterInputStreamWrapper(input, new Inflater(true)));
            default:
                throw new EdgeSocketConnectorException(
                        EdgeSocketConnectorErrorCode.PACKET_UNSUPPORTED_COMPRESSION,
                        "Unsupported packet compression type: " + compressionType);
        }
    }

    /**
     * Read all bytes from factory-created stream and map IO failures to connector error.
     *
     * @param inputStreamFactory stream factory for concrete compression implementation
     * @return fully decompressed bytes
     */
    private byte[] readAll(InputStreamFactory inputStreamFactory) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (InputStream inputStream = inputStreamFactory.create()) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = inputStream.read(buffer)) != -1) {
                    baos.write(buffer, 0, len);
                }
            }
            return baos.toByteArray();
        } catch (IOException ioException) {
            throw new EdgeSocketConnectorException(
                    EdgeSocketConnectorErrorCode.PACKET_DECODE_ERROR,
                    "Payload decompression failed",
                    ioException);
        }
    }

    /** Lazily creates compression-specific input streams for payload decoding. */
    private interface InputStreamFactory {
        /**
         * Create input stream for decompression.
         *
         * @return input stream over compressed payload
         * @throws IOException if stream init fails
         */
        InputStream create() throws IOException;
    }

    private static class GZIPInputStreamWrapper implements InputStreamFactory {
        private final byte[] input;

        private GZIPInputStreamWrapper(byte[] input) {
            this.input = input;
        }

        @Override
        public InputStream create() throws IOException {
            return new GZIPInputStream(new ByteArrayInputStream(input));
        }
    }

    private static class InflaterInputStreamWrapper implements InputStreamFactory {
        private final byte[] input;
        private final Inflater inflater;

        private InflaterInputStreamWrapper(byte[] input, Inflater inflater) {
            this.input = input;
            this.inflater = inflater;
        }

        @Override
        public InputStream create() {
            return new InflaterInputStream(new ByteArrayInputStream(input), inflater) {
                @Override
                public void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        inflater.end();
                    }
                }
            };
        }
    }
}
