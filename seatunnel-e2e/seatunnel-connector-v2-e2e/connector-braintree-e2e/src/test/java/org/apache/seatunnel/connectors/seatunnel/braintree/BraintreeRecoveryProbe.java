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
package org.apache.seatunnel.connectors.seatunnel.braintree;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Exercises exact mid-batch Braintree recovery from the packaged connector.
 *
 * <p>Actual provider calls use the same independent HTTPS peer as the Zeta suite.
 */
public final class BraintreeRecoveryProbe {
    private static final ObjectMapper JSON = new ObjectMapper();

    private BraintreeRecoveryProbe() {}

    /**
     * Captures an emitted row, serializes the checkpoint and verifies its precise replay position.
     *
     * <p>The zero mode checks unassigned, cancelled, invalid and terminal positions.
     */
    public static void main(String[] args) throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("environment", "Sandbox");
        options.put("merchant_id", "merchant_01");
        options.put("access_key", "synthetic-braintree-public");
        options.put("secret_key", "synthetic-braintree-private+&=%");
        options.put("resources", List.of("customers"));
        options.put("start_date", args[2]);
        options.put("end_date", args[3]);
        options.put("tls_trust_certificate", Files.readString(Path.of(args[0])));
        options.put("max_retries", 0);
        BraintreeConfig config = new BraintreeConfig(ReadonlyConfig.fromMap(options));
        if ("zero".equals(args[1])) {
            zero(config);
            return;
        }
        Rows before = new Rows();
        BraintreeSplit saved;
        try (BraintreeReader reader = opened(config, new Context())) {
            require(reader.snapshotState(0L).isEmpty(), "unassigned state must be empty");
            reader.addSplits(List.of(config.newSplit(Instant.now())));
            long expires = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (before.values.isEmpty() && System.nanoTime() < expires) {
                reader.pollNext(before);
                Thread.sleep(5);
            }
            require(before.values.size() == 1, "one row must precede the serialized checkpoint");
            require(
                    "first".equals(before.values.get(0).getField(1)),
                    "first ID must be emitted first");
            saved = reader.snapshotState(1L).get(0);
        }
        require(
                saved.row == 1
                        && saved.digest != null
                        && saved.ids.equals(List.of("first", "second")),
                "checkpoint retains the exact ID batch and row");
        byte[] bytes = serialize(saved);
        String raw = new String(bytes, StandardCharsets.ISO_8859_1);
        require(
                !raw.contains("synthetic-braintree-") && !raw.contains("initial"),
                "checkpoint must exclude credentials and provider records");
        BraintreeSplit restored;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            restored = (BraintreeSplit) in.readObject();
        }
        Rows after = new Rows();
        Context context = new Context();
        boolean failed = false;
        try (BraintreeReader reader = opened(config, context)) {
            reader.addSplits(List.of(restored));
            long expires = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (!context.done && System.nanoTime() < expires) {
                reader.pollNext(after);
                Thread.sleep(5);
            }
            require("resume".equals(args[1]), "a modified batch must fail");
            require(
                    context.done && after.values.size() == 1,
                    "only one row remains after restoration");
            SeaTunnelRow row = after.values.get(0);
            require(
                    "second".equals(row.getField(1))
                            && args[3].equals(row.getField(2))
                            && "second"
                                    .equals(
                                            JSON.readTree((String) row.getField(3))
                                                    .path("first_name")
                                                    .asText()),
                    "the exact suffix record must survive");
            terminal(config, reader.snapshotState(2L).get(0));
        } catch (BraintreeFailure ex) {
            require(
                    "changed".equals(args[1]) && ex.getMessage().contains("changed before replay"),
                    "changed batch must fail for its actual saved digest");
            require(after.values.isEmpty(), "rejected replay publishes no suffix");
            failed = true;
        }
        require(
                failed == "changed".equals(args[1]),
                "probe must observe the expected replay outcome");
    }

    private static void zero(BraintreeConfig config) throws Exception {
        BraintreeSplit finished = config.newSplit(Instant.now());
        finished.resourceIndex = config.resources.size();
        terminal(config, finished);
        Context idle = new Context();
        try (BraintreeReader reader = opened(config, idle)) {
            reader.handleNoMoreSplits();
            reader.pollNext(new Rows());
            require(
                    idle.done && reader.snapshotState(0L).isEmpty(),
                    "unassigned reader must finish");
        }
        try (BraintreeReader reader = new BraintreeReader(config, new Context())) {
            reader.cancel();
            reader.open();
            reader.addSplits(List.of(config.newSplit(Instant.now())));
            Rows rows = new Rows();
            reader.pollNext(rows);
            require(rows.values.isEmpty(), "cancellation before open is sticky");
        }
        rejected(config, new BraintreeSplit("changed-scope", config.start, finished.end));
        BraintreeSplit invalid = config.newSplit(Instant.now());
        invalid.row = -1;
        rejected(config, invalid);
    }

    private static void terminal(BraintreeConfig config, BraintreeSplit split) throws Exception {
        Context done = new Context();
        try (BraintreeReader reader = opened(config, done);
                ObjectInputStream in =
                        new ObjectInputStream(new ByteArrayInputStream(serialize(split)))) {
            reader.addSplits(List.of((BraintreeSplit) in.readObject()));
            Rows rows = new Rows();
            reader.pollNext(rows);
            require(done.done && rows.values.isEmpty(), "terminal saved state needs no request");
        }
    }

    private static void rejected(BraintreeConfig config, BraintreeSplit split) {
        try (BraintreeReader reader = opened(config, new Context())) {
            reader.addSplits(List.of(split));
        } catch (BraintreeFailure ex) {
            require(
                    ex.getMessage().contains("checkpoint"),
                    "checkpoint error must explain its scope");
            return;
        }
        throw new AssertionError("invalid position was accepted");
    }

    private static BraintreeReader opened(BraintreeConfig config, Context context) {
        BraintreeReader reader = new BraintreeReader(config, context);
        reader.open();
        return reader;
    }

    private static byte[] serialize(BraintreeSplit split) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(split);
        }
        return bytes.toByteArray();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    /**
     * Collects actual connector rows while providing the same checkpoint lock as an engine task.
     */
    private static final class Rows implements Collector<SeaTunnelRow> {
        private final Object lock = new Object();
        private final List<SeaTunnelRow> values = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow row) {
            values.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }

    /**
     * Records bounded completion from the real packaged reader without mocking its state.
     *
     * <p>The probe supplies no metrics service or enumerator-side behavior.
     */
    private static final class Context implements SourceReader.Context {
        private boolean done;

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {
            done = true;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}
