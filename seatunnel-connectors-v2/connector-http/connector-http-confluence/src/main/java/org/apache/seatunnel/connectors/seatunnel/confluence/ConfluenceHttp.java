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

package org.apache.seatunnel.connectors.seatunnel.confluence;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.impl.DefaultBHttpClientConnectionFactory;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpRequestExecutor;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/** A direct, non-logging HTTP GET transport. Credentials stay in the TLS request header. */
final class ConfluenceHttp {
    private static final ConnectionConfig CONNECTION_CONFIG =
            ConnectionConfig.custom()
                    .setMessageConstraints(
                            MessageConstraints.custom()
                                    .setMaxHeaderCount(64)
                                    .setMaxLineLength(8192)
                                    .build())
                    .build();

    private final ConfluenceConfig config;
    private final AtomicBoolean cancelled;
    private final AtomicReference<Socket> active = new AtomicReference<>();

    ConfluenceHttp(ConfluenceConfig config, AtomicBoolean cancelled) {
        this.config = config;
        this.cancelled = cancelled;
    }

    ConfluencePage fetch(String position) throws Retry {
        Socket socket = new Socket();
        active.set(socket);
        try (Socket connection = socket) {
            if (cancelled.get() || Thread.currentThread().isInterrupted()) {
                throw config.failure("request cancelled");
            }
            String host = config.origin.getHost();
            int port = ConfluenceConfig.port(config.origin);
            String tlsHost = host.startsWith("[") ? host.substring(1, host.length() - 1) : host;
            connection.connect(new InetSocketAddress(tlsHost, port), config.connectTimeout);
            connection.setSoTimeout(config.readTimeout);
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket tls =
                    (SSLSocket) factory.createSocket(connection, tlsHost, port, true)) {
                SSLParameters parameters = tls.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                tls.setSSLParameters(parameters);
                tls.startHandshake();
                return execute(tls, position);
            }
        } catch (SSLException e) {
            throw config.failure("TLS connection failed (details withheld)");
        } catch (HttpException e) {
            throw config.failure("malformed HTTP response (details withheld)");
        } catch (IOException e) {
            throw new Retry("transport failure", 0);
        } finally {
            active.compareAndSet(socket, null);
        }
    }

    private ConfluencePage execute(Socket socket, String position)
            throws IOException, HttpException, Retry {
        DefaultBHttpClientConnection connection =
                new DefaultBHttpClientConnectionFactory(CONNECTION_CONFIG).createConnection(socket);
        try {
            BasicHttpRequest request =
                    new BasicHttpRequest("GET", config.target(position), HttpVersion.HTTP_1_1);
            request.setHeader("Host", config.origin.getRawAuthority());
            request.setHeader("Accept", "application/json");
            request.setHeader("Accept-Encoding", "gzip");
            request.setHeader("Connection", "close");
            request.setHeader("Authorization", config.authorization());
            HttpResponse response =
                    new HttpRequestExecutor()
                            .execute(request, connection, HttpCoreContext.create());
            int status = response.getStatusLine().getStatusCode();
            if (status == 429
                    || status == 408
                    || status == 500
                    || status == 502
                    || status == 503
                    || status == 504) {
                throw new Retry(
                        "HTTP " + status, retryAfter(singleHeader(response, "Retry-After")));
            }
            if (status != 200) {
                throw config.failure(
                        "HTTP " + status + "; check the site, token and resource permissions");
            }
            validateFraming(response);
            byte[] body = readBody(response);
            return ConfluencePage.decode(config, body, response.getHeaders("Link"), position);
        } finally {
            // A rejected response may still be streaming. Do not drain its body to close it.
            try {
                connection.shutdown();
            } catch (IOException ignored) {
                // The owning socket also closes when fetch exits.
            }
        }
    }

    void abort() {
        Socket socket = active.getAndSet(null);
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The worker also closes the socket when it leaves fetch().
            }
        }
    }

    private void validateFraming(HttpResponse response) {
        String length = singleHeader(response, "Content-Length");
        String transfer = singleHeader(response, "Transfer-Encoding");
        if (transfer != null && (length != null || !"chunked".equalsIgnoreCase(transfer))) {
            throw config.failure("invalid HTTP response framing");
        }
        if (length != null && !length.matches("[0-9]{1,18}")) {
            throw config.failure("invalid HTTP response length");
        }
    }

    private byte[] readBody(HttpResponse response) throws IOException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw config.failure("missing response body");
        }
        String encoding = singleHeader(response, "Content-Encoding");
        InputStream input = entity.getContent();
        if ("gzip".equalsIgnoreCase(encoding)) {
            // HttpCore framing streams can drain on close. Close only the inflater here;
            // execute() shuts down the socket even when the size bound is hit.
            try (GZIPInputStream gzip =
                    new GZIPInputStream(
                            new FilterInputStream(input) {
                                @Override
                                public void close() {}
                            })) {
                return readBytes(gzip);
            } catch (ZipException e) {
                throw config.failure("malformed gzip response (body withheld)");
            }
        } else if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            throw config.failure("unsupported response content encoding");
        } else if (entity.getContentLength() > config.maxBytes) {
            throw config.failure("response exceeds max_response_bytes");
        }
        return readBytes(input);
    }

    private byte[] readBytes(InputStream body) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int count;
        while ((count = body.read(buffer)) != -1) {
            if (cancelled.get() || Thread.currentThread().isInterrupted()) {
                throw config.failure("request cancelled");
            }
            if (count > config.maxBytes - bytes.size()) {
                throw config.failure("response exceeds max_response_bytes");
            }
            bytes.write(buffer, 0, count);
        }
        return bytes.toByteArray();
    }

    private String singleHeader(HttpResponse response, String name) {
        Header[] values = response.getHeaders(name);
        if (values.length > 1) {
            throw config.failure("ambiguous " + name + " HTTP header");
        }
        return values.length == 0 ? null : values[0].getValue().trim();
    }

    private static long retryAfter(String header) {
        if (header == null || header.isEmpty()) {
            return 0;
        }
        if (header.chars().allMatch(c -> c >= '0' && c <= '9')) {
            long seconds = 0;
            for (int i = 0; i < header.length(); i++) {
                seconds = seconds * 10 + header.charAt(i) - '0';
                if (seconds > 600) {
                    return Long.MAX_VALUE;
                }
            }
            return seconds * 1000;
        }
        Date date = DateUtils.parseDate(header);
        return date == null ? 0 : Math.max(0, date.getTime() - System.currentTimeMillis());
    }

    static final class Retry extends Exception {
        private static final long serialVersionUID = 1L;
        final long delayMillis;

        Retry(String reason, long delayMillis) {
            super(reason);
            this.delayMillis = delayMillis;
        }
    }
}
