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

package org.apache.seatunnel.connectors.seatunnel.cbs;

import org.apache.http.HttpClientConnection;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Bounds proxy CONNECT refusals before HttpClient's native tunnel code buffers their entities.
 * Default protocol/continue behavior and bounded ordinary proxy-authentication challenges remain.
 */
final class CbsStatLineRequestExecutor extends HttpRequestExecutor {
    private static final int MAX_REFUSAL_BYTES = 65536;

    @Override
    protected HttpResponse doReceiveResponse(
            HttpRequest request, HttpClientConnection connection, HttpContext context)
            throws HttpException, IOException {
        HttpResponse response = super.doReceiveResponse(request, connection, context);
        HttpEntity entity = response.getEntity();
        if ("CONNECT".equalsIgnoreCase(request.getRequestLine().getMethod())
                && response.getStatusLine().getStatusCode() > 299
                && entity != null) {
            if (entity.getContentLength() > MAX_REFUSAL_BYTES) {
                throw refusal(connection);
            }
            response.setEntity(new Refusal(entity, connection));
        }
        return response;
    }

    private static IOException refusal(HttpClientConnection connection) {
        IOException failure = new IOException("CBS HTTP proxy refusal exceeds the byte limit");
        try {
            connection.shutdown();
        } catch (IOException cleanup) {
            failure.addSuppressed(cleanup);
        }
        return failure;
    }

    /**
     * Covers both native entity consumption and BufferedHttpEntity.writeTo. Merely wrapping
     * getContent would leave the native pre-reader buffer's writeTo path unrestricted.
     */
    private static final class Refusal extends HttpEntityWrapper {
        private final HttpClientConnection connection;

        private Refusal(HttpEntity entity, HttpClientConnection connection) {
            super(entity);
            this.connection = connection;
        }

        @Override
        public InputStream getContent() throws IOException {
            return new RefusalInput(super.getContent(), connection);
        }

        @Override
        public void writeTo(OutputStream output) throws IOException {
            try (InputStream input = getContent()) {
                byte[] buffer = new byte[8192];
                int count;
                while ((count = input.read(buffer)) != -1) {
                    output.write(buffer, 0, count);
                }
            }
        }
    }

    /**
     * Shuts down the owning connection before an oversized refusal is closed, preventing native
     * cleanup from draining an unbounded proxy body after rejecting it.
     */
    private static final class RefusalInput extends FilterInputStream {
        private final HttpClientConnection connection;
        private int consumed;
        private boolean closed;

        private RefusalInput(InputStream input, HttpClientConnection connection) {
            super(input);
            this.connection = connection;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value >= 0) {
                charge(1);
            }
            return value;
        }

        @Override
        public int read(byte[] value, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            int count = in.read(value, offset, Math.min(length, MAX_REFUSAL_BYTES - consumed + 1));
            if (count > 0) {
                charge(count);
            }
            return count;
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            // Native authentication may consume an entity by closing it without explicit reads.
            try (InputStream original = in) {
                byte[] buffer = new byte[8192];
                while (read(buffer) != -1) {
                    // The same refusal bound also applies to this cleanup drain.
                }
            }
        }

        private void charge(int bytes) throws IOException {
            if (bytes > MAX_REFUSAL_BYTES - consumed) {
                throw refusal(connection);
            }
            consumed += bytes;
        }
    }
}
