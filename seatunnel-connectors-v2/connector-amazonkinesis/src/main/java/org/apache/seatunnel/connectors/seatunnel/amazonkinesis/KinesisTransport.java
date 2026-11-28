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

package org.apache.seatunnel.connectors.seatunnel.amazonkinesis;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpRequestExecutor;

import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/** Quiet, abortable transport for the generated SDK CBOR/JSON client. */
final class KinesisTransport implements SdkHttpClient {
    static final int MAX_WIRE_BYTES = 16 * 1024 * 1024;
    private static final MessageConstraints HEADERS =
            MessageConstraints.custom().setMaxLineLength(16384).setMaxHeaderCount(64).build();

    private final int socketTimeout;
    private final Pattern host;
    private final Set<Call> inFlight = new HashSet<>();
    private boolean closed;

    KinesisTransport(String region, int socketTimeout) {
        this.socketTimeout = socketTimeout;
        host =
                Pattern.compile(
                        "kinesis(?:-fips)?\\."
                                + Pattern.quote(region)
                                + "\\.(?:amazonaws\\.com(?:\\.cn)?|api\\.aws|api\\.amazonwebservices\\.com\\.cn"
                                + "|c2s\\.ic\\.gov|sc2s\\.sgov\\.gov|cloud\\.adc-e\\.uk|csp\\.hci\\.ic\\.gov)");
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
        return new Call(request);
    }

    @Override
    public synchronized void close() {
        closed = true;
        inFlight.forEach(Call::abort);
    }

    private final class Call implements ExecutableHttpRequest {
        private final HttpExecuteRequest execution;
        private Socket socket;
        private boolean aborted;

        private Call(HttpExecuteRequest execution) {
            this.execution = execution;
        }

        @Override
        public HttpExecuteResponse call() throws IOException {
            synchronized (KinesisTransport.this) {
                if (closed) {
                    throw new IOException("Kinesis transport is closed");
                }
                inFlight.add(this);
            }
            try (Socket raw = new Socket(Proxy.NO_PROXY)) {
                synchronized (this) {
                    if (aborted) {
                        throw new IOException("Kinesis request was canceled");
                    }
                    socket = raw;
                }
                SdkHttpRequest sdk = execution.httpRequest();
                if (!"https".equals(sdk.protocol())
                        || !"/".equals(sdk.encodedPath())
                        || sdk.port() != 443
                        || !host.matcher(sdk.host()).matches()
                        || !sdk.rawQueryParameters().isEmpty()) {
                    throw new IOException("Invalid Kinesis transport endpoint");
                }
                raw.connect(new InetSocketAddress(sdk.host(), sdk.port()), socketTimeout);
                raw.setSoTimeout(socketTimeout);
                SSLSocketFactory tls = (SSLSocketFactory) SSLSocketFactory.getDefault();
                try (SSLSocket ssl =
                        (SSLSocket) tls.createSocket(raw, sdk.host(), sdk.port(), true)) {
                    SSLParameters sslParameters = ssl.getSSLParameters();
                    sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
                    ssl.setSSLParameters(sslParameters);
                    ssl.startHandshake();
                    return exchange(sdk, ssl, raw);
                }
            } finally {
                synchronized (KinesisTransport.this) {
                    inFlight.remove(this);
                }
            }
        }

        private HttpExecuteResponse exchange(SdkHttpRequest sdk, Socket tls, Socket raw)
                throws IOException {
            BasicHttpEntityEnclosingRequest request =
                    new BasicHttpEntityEnclosingRequest("POST", "/");
            byte[] data = new byte[0];
            if (execution.contentStreamProvider().isPresent()) {
                try (InputStream body = execution.contentStreamProvider().get().newStream()) {
                    data = readAll(body);
                }
            }
            sdk.headers()
                    .forEach(
                            (name, values) -> {
                                if (!name.equalsIgnoreCase("Content-Length")
                                        && !name.equalsIgnoreCase("Connection")
                                        && !name.equalsIgnoreCase("Transfer-Encoding")) {
                                    values.forEach(value -> request.addHeader(name, value));
                                }
                            });
            request.addHeader("Connection", "close");
            request.addHeader("Content-Length", Integer.toString(data.length));
            request.setEntity(new ByteArrayEntity(data));
            // The socket owns this connection. Closing it first on an incomplete entity prevents
            // draining.
            DefaultBHttpClientConnection connection =
                    new DefaultBHttpClientConnection(8192, null, null, HEADERS);
            connection.bind(tls);
            try {
                org.apache.http.HttpResponse response =
                        new HttpRequestExecutor()
                                .execute(request, connection, HttpCoreContext.create());
                SdkHttpResponse.Builder metadata =
                        SdkHttpResponse.builder()
                                .statusCode(response.getStatusLine().getStatusCode());
                Header encoding = response.getFirstHeader("Content-Encoding");
                boolean gzip = encoding != null && "gzip".equalsIgnoreCase(encoding.getValue());
                if (encoding != null
                        && !gzip
                        && !"identity".equalsIgnoreCase(encoding.getValue())) {
                    throw new IOException("Unsupported Kinesis response encoding");
                }
                for (Header header : response.getAllHeaders()) {
                    if (!gzip
                            || (!header.getName().equalsIgnoreCase("Content-Encoding")
                                    && !header.getName().equalsIgnoreCase("Content-Length"))) {
                        metadata.appendHeader(header.getName(), header.getValue());
                    }
                }
                HttpEntity entity = response.getEntity();
                byte[] reply = new byte[0];
                if (entity != null) {
                    InputStream body = new WireLimit(entity.getContent());
                    try {
                        if (gzip) {
                            body = new GZIPInputStream(body);
                        }
                        reply = readAll(body);
                    } catch (IOException e) {
                        closeSocket(raw);
                        throw e;
                    } finally {
                        try {
                            body.close();
                        } catch (IOException ignored) {
                            // The transport owns the socket and closes it on every path.
                        }
                    }
                }
                return HttpExecuteResponse.builder()
                        .response(metadata.build())
                        .responseBody(AbortableInputStream.create(new ByteArrayInputStream(reply)))
                        .build();
            } catch (HttpException e) {
                throw new IOException("Invalid Kinesis HTTP response");
            }
        }

        @Override
        public synchronized void abort() {
            aborted = true;
            closeSocket(socket);
        }
    }

    private static byte[] readAll(InputStream input) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int count;
        while ((count = input.read(buffer)) != -1) {
            if (count > MAX_WIRE_BYTES - bytes.size()) {
                throw new IOException("Kinesis response or request exceeds the transport limit");
            }
            bytes.write(buffer, 0, count);
        }
        return bytes.toByteArray();
    }

    private static void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Cancellation is best effort after the connection has been discarded.
            }
        }
    }

    /** Compressed bytes and decompressed bytes have independent finite budgets. */
    private static final class WireLimit extends FilterInputStream {
        private long count;

        private WireLimit(InputStream input) {
            super(input);
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int read = super.read(buffer, offset, length);
            if (read > 0) {
                increment(read);
            }
            return read;
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value >= 0) {
                increment(1);
            }
            return value;
        }

        private void increment(int size) throws IOException {
            count += size;
            if (count > MAX_WIRE_BYTES) {
                throw new IOException("Kinesis wire response exceeds the transport limit");
            }
        }
    }
}
