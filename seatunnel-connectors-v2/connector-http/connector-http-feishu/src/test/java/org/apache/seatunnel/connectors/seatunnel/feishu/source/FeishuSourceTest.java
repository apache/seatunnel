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

package org.apache.seatunnel.connectors.seatunnel.feishu.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumeratorState;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class FeishuSourceTest {

    private static final String BASE_TOKEN = "base-debug-credential-93";
    private static final String CLIENT_SECRET = "app-debug-credential-87";
    private static final String BEARER_TOKEN = "t-tenant-debug-credential-65";
    private static final String CURSOR = "cursor-debug-credential-52";
    private static final String RESPONSE_MARKER = "private-response-body-47";

    @Test
    void assignsExactlyOneInitialSplitAfterAnEarlyEmptyCheckpoint() throws Exception {
        FeishuSource source = source("https://open.feishu.cn");
        Collector<SeaTunnelRow> output = collector();
        List<SingleSplit> earlySnapshot;
        try (SourceReader<SeaTunnelRow, SingleSplit> reader =
                source.createReader(mock(SourceReader.Context.class))) {
            reader.open();
            reader.pollNext(output);
            earlySnapshot = reader.snapshotState(1L);
            assertTrue(earlySnapshot.isEmpty());
        }
        verifyNoInteractions(output);

        SourceSplitEnumerator.Context<SingleSplit> context = enumeratorContext();
        try (SourceSplitEnumerator<SingleSplit, SingleSplitEnumeratorState> coordinator =
                        source.restoreEnumerator(context, new SingleSplitEnumeratorState());
                SourceReader<SeaTunnelRow, SingleSplit> restoredReader =
                        source.createReader(mock(SourceReader.Context.class))) {
            coordinator.open();
            restoredReader.open();
            coordinator.addSplitsBack(earlySnapshot, 0);
            restoredReader.pollNext(output);
            assertTrue(restoredReader.snapshotState(2L).isEmpty());

            ArgumentCaptor<SingleSplit> assignments = ArgumentCaptor.forClass(SingleSplit.class);
            forwardAssignments(context, restoredReader);
            when(context.registeredReaders()).thenReturn(Collections.singleton(0));
            coordinator.registerReader(0);
            coordinator.run();
            coordinator.addSplitsBack(earlySnapshot, 0);
            coordinator.registerReader(0);
            coordinator.run();

            verify(context).assignSplit(eq(0), assignments.capture());
            assertNull(
                    assignments.getValue().getState(), "A blank snapshot starts the initial scan");
            assertEquals(
                    1,
                    restoredReader.snapshotState(3L).size(),
                    "The real reader received the split");
        }
        verifyNoInteractions(output);
    }

    @Test
    void terminalCheckpointRestoresWithoutHttpAndDebugLogsOmitCredentials() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer provider = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        provider.createContext("/", exchange -> respond(exchange, requests.incrementAndGet()));
        LoggerContext logging = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = logging.getConfiguration();
        String category = "org.apache.http";
        LoggerConfig previous = configuration.getLoggers().get(category);
        List<String> messages = new CopyOnWriteArrayList<>();
        AbstractAppender capture =
                new AbstractAppender(
                        "feishu-http-log-capture", null, null, true, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        capture.start();
        LoggerConfig diagnostic = new LoggerConfig(category, Level.DEBUG, false);
        diagnostic.addAppender(capture, Level.DEBUG, null);
        configuration.removeLogger(category);
        configuration.addLogger(category, diagnostic);
        logging.updateLoggers();

        provider.start();
        try {
            Log requestLineLogger =
                    LogFactory.getLog("org.apache.http.impl.execchain.MainClientExec");
            assertTrue(requestLineLogger.isDebugEnabled(), "Request-line DEBUG must be enabled");
            requestLineLogger.debug("feishu-transport-debug-control");
            FeishuSource source = source("http://127.0.0.1:" + provider.getAddress().getPort());
            List<SingleSplit> terminal = reachEndAndSnapshot(source);
            assertEquals(3, requests.get(), "One authentication and two actual record requests");

            SourceReader.Context restoredContext = mock(SourceReader.Context.class);
            Collector<SeaTunnelRow> restoredOutput = collector();
            SourceSplitEnumerator.Context<SingleSplit> coordinatorContext = enumeratorContext();
            try (SourceSplitEnumerator<SingleSplit, SingleSplitEnumeratorState> coordinator =
                            source.restoreEnumerator(
                                    coordinatorContext, new SingleSplitEnumeratorState());
                    SourceReader<SeaTunnelRow, SingleSplit> restored =
                            source.createReader(restoredContext)) {
                forwardAssignments(coordinatorContext, restored);
                coordinator.open();
                restored.open();
                coordinator.addSplitsBack(terminal, 0);
                when(coordinatorContext.registeredReaders()).thenReturn(Collections.singleton(0));
                coordinator.registerReader(0);
                coordinator.run();
                restored.pollNext(restoredOutput);
                restored.pollNext(restoredOutput);

                verify(coordinatorContext).assignSplit(0, terminal.get(0));
                verify(restoredContext).signalNoMoreElement();
                verifyNoInteractions(restoredOutput);
                assertEquals(3, requests.get(), "Terminal restore must issue no HTTP calls");
            }

            String debug = String.join("\n", messages);
            assertTrue(debug.contains("feishu-transport-debug-control"), "DEBUG capture is active");
            assertTrue(
                    debug.contains("Connection request:"),
                    "The real request produced transport diagnostics");
            assertFalse(debug.contains(BASE_TOKEN), "Provider path token leaked");
            assertFalse(debug.contains(CURSOR), "Page cursor leaked");
            assertFalse(debug.contains(CLIENT_SECRET), "Application secret leaked");
            assertFalse(debug.contains(BEARER_TOKEN), "Tenant bearer leaked");
            assertFalse(debug.contains(RESPONSE_MARKER), "Provider response body leaked");
        } finally {
            provider.stop(0);
            configuration.removeLogger(category);
            if (previous != null) {
                configuration.addLogger(category, previous);
            }
            logging.updateLoggers();
            capture.stop();
        }
    }

    private static List<SingleSplit> reachEndAndSnapshot(FeishuSource source) throws Exception {
        SourceReader.Context context = mock(SourceReader.Context.class);
        AtomicBoolean ended = new AtomicBoolean();
        doAnswer(
                        invocation -> {
                            ended.set(true);
                            return null;
                        })
                .when(context)
                .signalNoMoreElement();
        Collector<SeaTunnelRow> output = collector();
        when(output.getCheckpointLock()).thenReturn(new Object());
        try (SourceReader<SeaTunnelRow, SingleSplit> reader = source.createReader(context)) {
            reader.open();
            reader.addSplits(Collections.singletonList(new SingleSplit(null)));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!ended.get() && System.nanoTime() - deadline < 0L) {
                reader.pollNext(output);
                Thread.sleep(10L);
            }
            assertTrue(ended.get(), "The original source must finish the provider scan");
            ArgumentCaptor<SeaTunnelRow> row = ArgumentCaptor.forClass(SeaTunnelRow.class);
            verify(output).collect(row.capture());
            assertEquals("rec-terminal", row.getValue().getField(0));
            List<SingleSplit> terminal = reader.snapshotState(4L);
            assertEquals(1, terminal.size());
            return terminal;
        }
    }

    private static void respond(HttpExchange exchange, int request) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            JsonNode body = JsonUtils.readTree(exchange.getRequestBody());
            String response = "{\"code\":171";
            if (request == 1
                    && "/open-apis/auth/v3/tenant_access_token/internal".equals(path)
                    && CLIENT_SECRET.equals(body.path("app_secret").asText())) {
                response =
                        "{\"code\":0,\"tenant_access_token\":\""
                                + BEARER_TOKEN
                                + "\",\"expire\":7200";
            } else if (path.equals(
                            "/open-apis/bitable/v1/apps/"
                                    + BASE_TOKEN
                                    + "/tables/tbl-debug/records/search")
                    && ("Bearer " + BEARER_TOKEN)
                            .equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                String query = exchange.getRequestURI().getQuery();
                if (request == 2 && "page_size=100".equals(query)) {
                    response =
                            "{\"code\":0,\"data\":{\"has_more\":true,\"page_token\":\""
                                    + CURSOR
                                    + "\",\"items\":[]}";
                } else if (request == 3 && ("page_size=100&page_token=" + CURSOR).equals(query)) {
                    response =
                            "{\"code\":0,\"data\":{\"has_more\":false,\"items\":[{\"record_id\":\"rec-terminal\"}]}";
                }
            }
            byte[] payload =
                    (response + ",\"msg\":\"" + RESPONSE_MARKER + "\"}")
                            .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, payload.length);
            exchange.getResponseBody().write(payload);
        } finally {
            exchange.close();
        }
    }

    private static FeishuSource source(String origin) {
        Map<String, Object> config = new HashMap<>();
        config.put("app_id", "cli_debug");
        config.put("client_secret", CLIENT_SECRET);
        config.put("token", BASE_TOKEN);
        config.put("table_id", "tbl-debug");
        config.put("api_base_url", origin);
        config.put("allow_insecure_http", true);
        return new FeishuSource(ReadonlyConfig.fromMap(config));
    }

    private static void forwardAssignments(
            SourceSplitEnumerator.Context<SingleSplit> context,
            SourceReader<SeaTunnelRow, SingleSplit> reader) {
        doAnswer(
                        invocation -> {
                            reader.addSplits(Collections.singletonList(invocation.getArgument(1)));
                            return null;
                        })
                .when(context)
                .assignSplit(eq(0), any(SingleSplit.class));
    }

    @SuppressWarnings("unchecked")
    private static Collector<SeaTunnelRow> collector() {
        return mock(Collector.class);
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<SingleSplit> enumeratorContext() {
        return mock(SourceSplitEnumerator.Context.class);
    }
}
