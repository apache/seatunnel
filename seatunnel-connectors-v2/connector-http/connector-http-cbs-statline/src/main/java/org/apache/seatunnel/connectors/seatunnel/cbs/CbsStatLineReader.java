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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonToken;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

final class CbsStatLineReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private static final int HTTP_TIMEOUT_MS = 30_000;
    private static final int MAX_PAGES = 10_000;
    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private final URI start;
    private final String dataset;
    private final int maxRows;
    private final SingleSplitReaderContext context;
    private final AtomicReference<HttpGet> inFlight = new AtomicReference<>();
    private volatile boolean cancelled;
    private CloseableHttpClient client;
    private long emitted;

    CbsStatLineReader(URI start, String dataset, int maxRows, SingleSplitReaderContext context) {
        this.start = start;
        this.dataset = dataset;
        this.maxRows = maxRows;
        this.context = context;
    }

    @Override
    public void open() {
        client =
                HttpClients.custom()
                        .useSystemProperties()
                        .disableRedirectHandling()
                        .disableAutomaticRetries()
                        .disableCookieManagement()
                        .setDefaultRequestConfig(
                                RequestConfig.custom()
                                        .setConnectTimeout(HTTP_TIMEOUT_MS)
                                        .setSocketTimeout(HTTP_TIMEOUT_MS)
                                        .build())
                        .build();
    }

    @Override
    public void cancel() {
        cancelled = true;
        HttpGet request = inFlight.get();
        if (request != null) {
            request.abort();
        }
    }

    @Override
    public void close() throws IOException {
        cancel();
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws IOException {
        URI next = start;
        Set<URI> visited = new HashSet<>();
        while (!cancelled && next != null && !limitReached()) {
            if (visited.size() == MAX_PAGES || !visited.add(next)) {
                throw new IOException("CBS OData continuation repeated or exceeded the page limit");
            }
            next = fetchPage(next, output);
        }
        if (!cancelled) {
            context.signalNoMoreElement();
        }
    }

    private boolean limitReached() {
        return maxRows > 0 && emitted >= maxRows;
    }

    private URI fetchPage(URI page, Collector<SeaTunnelRow> output) throws IOException {
        HttpGet request = new HttpGet(page);
        request.setHeader("Accept", "application/json");
        inFlight.set(request);
        try {
            if (cancelled) {
                return null;
            }
            try (CloseableHttpResponse response = client.execute(request)) {
                int status = response.getStatusLine().getStatusCode();
                if (status != 200 || response.getEntity() == null) {
                    throw new IOException("CBS OData Observations request returned HTTP " + status);
                }
                try (JsonParser parser =
                        MAPPER.getFactory().createParser(response.getEntity().getContent())) {
                    return parsePage(page, parser, output);
                }
            }
        } catch (IOException error) {
            if (cancelled) {
                return null;
            }
            throw error;
        } finally {
            inFlight.compareAndSet(request, null);
        }
    }

    private URI parsePage(URI page, JsonParser parser, Collector<SeaTunnelRow> output)
            throws IOException {
        if (parser.nextToken() != JsonToken.START_OBJECT) {
            throw new IOException("CBS OData response must be an object");
        }
        URI next = null;
        boolean sawValue = false;
        boolean sawNext = false;
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String field = parser.getCurrentName();
            JsonToken token = parser.nextToken();
            if ("value".equals(field)) {
                if (sawValue || token != JsonToken.START_ARRAY) {
                    throw new IOException("CBS OData response must contain one value array");
                }
                sawValue = true;
                while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
                    if (token != JsonToken.START_OBJECT) {
                        throw new IOException("CBS OData observations must be objects");
                    }
                    emit(MAPPER.readTree(parser), output);
                    if (cancelled || limitReached()) {
                        return null;
                    }
                }
            } else if ("@odata.nextLink".equals(field)) {
                if (sawNext || (token != JsonToken.VALUE_NULL && token != JsonToken.VALUE_STRING)) {
                    throw new IOException("CBS OData nextLink must be a URL string or null");
                }
                sawNext = true;
                if (token == JsonToken.VALUE_STRING) {
                    next = continuation(page, parser.getText());
                }
            } else {
                parser.skipChildren();
            }
        }
        if (!sawValue
                || parser.currentToken() != JsonToken.END_OBJECT
                || parser.nextToken() != null) {
            throw new IOException("Invalid CBS OData response envelope");
        }
        return next;
    }

    private void emit(JsonNode observation, Collector<SeaTunnelRow> output) throws IOException {
        JsonNode id = observation.path("Id");
        JsonNode measure = observation.path("Measure");
        JsonNode value = observation.path("Value");
        if (!id.isIntegralNumber()
                || !id.canConvertToLong()
                || !measure.isTextual()
                || (!value.isNull() && !value.isMissingNode() && !value.isNumber())) {
            throw new IOException("Invalid CBS OData observation Id, Measure, or Value");
        }
        output.collect(
                new SeaTunnelRow(
                        new Object[] {
                            dataset,
                            id.longValue(),
                            measure.textValue(),
                            value.isNumber() ? value.asText() : null,
                            optionalText(observation, "StringValue"),
                            optionalText(observation, "ValueAttribute"),
                            observation.toString()
                        }));
        emitted++;
    }

    private static String optionalText(JsonNode observation, String field) throws IOException {
        JsonNode value = observation.path(field);
        if (value.isNull() || value.isMissingNode()) {
            return null;
        }
        if (!value.isTextual()) {
            throw new IOException("CBS OData observation field " + field + " must be text or null");
        }
        return value.textValue();
    }

    static URI continuation(URI current, String value) throws IOException {
        final URI link;
        try {
            URI reference = URI.create(value);
            // URI.resolve treats query-only references like relative path segments.
            link =
                    value.startsWith("?")
                            ? URI.create(
                                    current.getScheme()
                                            + "://"
                                            + current.getRawAuthority()
                                            + current.getRawPath()
                                            + value)
                            : current.resolve(reference);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid CBS OData continuation URL");
        }
        if (value.isEmpty()
                || !current.getScheme().equalsIgnoreCase(link.getScheme())
                || !current.getHost().equalsIgnoreCase(link.getHost())
                || port(current) != port(link)
                || link.getRawUserInfo() != null
                || link.getRawFragment() != null
                || !current.getRawPath().equals(link.getRawPath())) {
            throw new IOException("CBS OData continuation must target the same dataset and origin");
        }
        return link;
    }

    private static int port(URI uri) {
        return uri.getPort() < 0
                ? ("https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80)
                : uri.getPort();
    }
}
