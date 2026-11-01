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

package org.apache.seatunnel.connectors.seatunnel.kafka.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSemantics;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.ASSIGN_PARTITIONS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.BOOTSTRAP_SERVERS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.KAFKA_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.SEMANTICS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSinkOptions.TRANSACTION_PREFIX;

/** KafkaSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Kafka. */
public class KafkaSinkWriter implements SinkWriter<SeaTunnelRow, KafkaCommitInfo, KafkaSinkState> {

    /** Job-level prefix persisted in every checkpoint for legacy restore compatibility. */
    private String transactionPrefix;

    /** Transaction namespace exclusively adopted by this writer from its assigned restore state. */
    private final String writerTransactionPrefix;

    /** Sequence of the transaction currently open in {@link #writerTransactionPrefix}. */
    private long currentTransactionSequence;

    private SeaTunnelRowType seaTunnelRowType;

    private final KafkaProduceSender<byte[], byte[]> kafkaProducerSender;
    private final SeaTunnelRowSerializer<byte[], byte[]> seaTunnelRowSerializer;

    private static final int PREFIX_RANGE = 10000;

    /**
     * Current writer-namespace grammar. The optional relocation suffix records the writer that
     * adopted the namespace and the engine checkpoint that assigned it.
     */
    private static final Pattern WRITER_NAMESPACE_PATTERN =
            Pattern.compile("(g[0-9a-z]+)w(\\d+)(?:r(\\d+)c(\\d+))?");

    public KafkaSinkWriter(
            SinkWriter.Context context,
            SeaTunnelRowType seaTunnelRowType,
            ReadonlyConfig pluginConfig,
            List<KafkaSinkState> kafkaStates) {
        this(
                context,
                seaTunnelRowType,
                pluginConfig,
                kafkaStates,
                resolveTransactionPrefix(pluginConfig),
                generateWriterNamespaceGeneration());
    }

    KafkaSinkWriter(
            SinkWriter.Context context,
            SeaTunnelRowType seaTunnelRowType,
            ReadonlyConfig pluginConfig,
            List<KafkaSinkState> kafkaStates,
            String transactionPrefix,
            String writerNamespaceGeneration) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.transactionPrefix = transactionPrefix;

        String newWriterNamespace =
                generateWriterNamespace(writerNamespaceGeneration, context.getIndexOfSubtask());
        RestorePlan restorePlan =
                restoreState(kafkaStates, newWriterNamespace, context.getIndexOfSubtask());
        this.writerTransactionPrefix = restorePlan.writerTransactionPrefix;
        this.currentTransactionSequence = restorePlan.nextTransactionSequence;
        this.seaTunnelRowSerializer = KafkaSinkSerializer.create(pluginConfig, seaTunnelRowType);
        if (KafkaSemantics.EXACTLY_ONCE.equals(getKafkaSemantics(pluginConfig))) {
            KafkaTransactionSender<byte[], byte[]> transactionSender =
                    new KafkaTransactionSender<>(
                            this.transactionPrefix,
                            this.writerTransactionPrefix,
                            getKafkaProperties(pluginConfig));
            this.kafkaProducerSender = transactionSender;
            abortRestoredTransactions(transactionSender, restorePlan);
            this.kafkaProducerSender.beginTransaction(
                    generateTransactionId(
                            this.writerTransactionPrefix, this.currentTransactionSequence));
        } else {
            this.kafkaProducerSender =
                    new KafkaNoTransactionSender<>(getKafkaProperties(pluginConfig));
        }
    }

    @Override
    public void write(SeaTunnelRow element) {
        ProducerRecord<byte[], byte[]> producerRecord =
                seaTunnelRowSerializer.serializeRow(element);
        kafkaProducerSender.send(producerRecord);
    }

    @Override
    public List<KafkaSinkState> snapshotState(long checkpointId) {
        List<KafkaSinkState> states = kafkaProducerSender.snapshotState(checkpointId);
        this.currentTransactionSequence++;
        this.kafkaProducerSender.beginTransaction(
                generateTransactionId(
                        this.writerTransactionPrefix, this.currentTransactionSequence));
        return states;
    }

    @Override
    public Optional<KafkaCommitInfo> prepareCommit() {
        return kafkaProducerSender.prepareCommit();
    }

    @Override
    public void abortPrepare() {
        kafkaProducerSender.abortTransaction();
    }

    @Override
    public void close() {
        try {
            kafkaProducerSender.close();
        } catch (Exception e) {
            throw new KafkaConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "Close kafka sink writer error",
                    e);
        }
    }

    private Properties getKafkaProperties(ReadonlyConfig pluginConfig) {
        Properties kafkaProperties = new Properties();
        if (pluginConfig.get(KAFKA_CONFIG) != null) {
            pluginConfig.get(KAFKA_CONFIG).forEach((key, value) -> kafkaProperties.put(key, value));
        }

        if (pluginConfig.get(ASSIGN_PARTITIONS) != null) {
            kafkaProperties.put(
                    ProducerConfig.PARTITIONER_CLASS_CONFIG,
                    "org.apache.seatunnel.connectors.seatunnel.kafka.sink.MessageContentPartitioner");
            kafkaProperties.put(
                    MessageContentPartitioner.ASSIGN_PARTITIONS_CONFIG,
                    pluginConfig.get(ASSIGN_PARTITIONS));
        }

        kafkaProperties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, pluginConfig.get(BOOTSTRAP_SERVERS));
        kafkaProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return kafkaProperties;
    }

    private KafkaSemantics getKafkaSemantics(ReadonlyConfig pluginConfig) {
        if (pluginConfig.get(SEMANTICS) != null) {
            return pluginConfig.get(SEMANTICS);
        }
        return KafkaSemantics.NON;
    }

    protected static String generateTransactionId(
            String transactionPrefix, long transactionSequence) {
        return transactionPrefix + "-" + transactionSequence;
    }

    static String resolveTransactionPrefix(ReadonlyConfig pluginConfig) {
        if (pluginConfig.get(TRANSACTION_PREFIX) != null) {
            return pluginConfig.get(TRANSACTION_PREFIX);
        }
        Random random = new Random();
        return String.format("SeaTunnel%04d", random.nextInt(PREFIX_RANGE));
    }

    static String generateWriterNamespaceGeneration() {
        return "g"
                + Long.toUnsignedString(
                        UUID.randomUUID().getLeastSignificantBits(), Character.MAX_RADIX);
    }

    private static String generateWriterNamespace(
            String writerNamespaceGeneration, int writerIndex) {
        return writerNamespaceGeneration + "w" + writerIndex;
    }

    private static String generateWriterTransactionPrefix(
            String transactionPrefix, String writerNamespace) {
        return transactionPrefix + "-" + writerNamespace;
    }

    /**
     * Builds an engine-neutral restore plan from the states assigned to this writer.
     *
     * <p>A restoring writer selects one non-legacy namespace from its own assigned states. Since
     * the engine assigns each old writer state to exactly one new writer, no sibling can select or
     * clean that namespace concurrently. A same-generation base namespace that moved to another
     * writer is deterministically relocated after cleanup, so a later stateless writer can safely
     * reuse its base slot. If there is no writer-specific state, a fresh namespace is derived from
     * a sink-level generation token and the current stable subtask index. The token is serialized
     * with the sink, so task retries reuse it.
     */
    private RestorePlan restoreState(
            List<KafkaSinkState> states, String newWriterNamespace, int currentWriterIndex) {
        List<RestoredTransaction> restoredTransactions = new ArrayList<>(states.size());
        for (KafkaSinkState state : states) {
            restoredTransactions.add(parseRestoredTransaction(state));
        }

        if (restoredTransactions.isEmpty()) {
            return new RestorePlan(
                    generateWriterTransactionPrefix(this.transactionPrefix, newWriterNamespace),
                    1L,
                    restoredTransactions);
        }

        Optional<RestoredTransaction> matchingWriterNamespace =
                restoredTransactions.stream()
                        .filter(transaction -> !transaction.legacy)
                        .filter(
                                transaction ->
                                        transaction.transactionIdPrefix.equals(
                                                generateWriterTransactionPrefix(
                                                        transaction.transactionPrefix,
                                                        newWriterNamespace)))
                        .min(Comparator.comparing(transaction -> transaction.transactionIdPrefix));
        Optional<RestoredTransaction> adoptedWriterNamespace =
                matchingWriterNamespace.isPresent()
                        ? matchingWriterNamespace
                        : restoredTransactions.stream()
                                .filter(transaction -> !transaction.legacy)
                                .min(
                                        Comparator.comparing(
                                                        (RestoredTransaction transaction) ->
                                                                transaction.transactionIdPrefix)
                                                .thenComparing(
                                                        transaction ->
                                                                transaction.transactionPrefix));

        if (adoptedWriterNamespace.isPresent()) {
            RestoredTransaction adopted = adoptedWriterNamespace.get();
            this.transactionPrefix = adopted.transactionPrefix;
            if (shouldRelocateWriterNamespace(adopted.writerNamespace, newWriterNamespace)) {
                String relocatedWriterNamespace =
                        relocateWriterNamespace(
                                adopted.writerNamespace, currentWriterIndex, adopted.checkpointId);
                return new RestorePlan(
                        generateWriterTransactionPrefix(
                                this.transactionPrefix, relocatedWriterNamespace),
                        1L,
                        restoredTransactions);
            }
            long nextTransactionSequence =
                    restoredTransactions.stream()
                                    .filter(
                                            transaction ->
                                                    transaction.transactionIdPrefix.equals(
                                                            adopted.transactionIdPrefix))
                                    .mapToLong(transaction -> transaction.transactionSequence)
                                    .max()
                                    .orElseThrow(IllegalStateException::new)
                            + 1;
            return new RestorePlan(
                    adopted.transactionIdPrefix, nextTransactionSequence, restoredTransactions);
        }

        RestoredTransaction adoptedLegacyPrefix =
                restoredTransactions.stream()
                        .min(Comparator.comparing(transaction -> transaction.transactionPrefix))
                        .orElseThrow(IllegalStateException::new);
        this.transactionPrefix = adoptedLegacyPrefix.transactionPrefix;
        return new RestorePlan(
                generateWriterTransactionPrefix(this.transactionPrefix, newWriterNamespace),
                1L,
                restoredTransactions);
    }

    private static boolean shouldRelocateWriterNamespace(
            String restoredWriterNamespace, String newWriterNamespace) {
        Matcher restoredMatcher = WRITER_NAMESPACE_PATTERN.matcher(restoredWriterNamespace);
        Matcher newMatcher = WRITER_NAMESPACE_PATTERN.matcher(newWriterNamespace);
        if (!restoredMatcher.matches() || !newMatcher.matches()) {
            throw new IllegalStateException("Cannot compare Kafka writer namespaces");
        }
        return restoredMatcher.group(1).equals(newMatcher.group(1))
                && restoredMatcher.group(3) == null
                && !restoredWriterNamespace.equals(newWriterNamespace);
    }

    private static String relocateWriterNamespace(
            String restoredWriterNamespace, int currentWriterIndex, long restoredCheckpointId) {
        if (restoredCheckpointId < 0) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot relocate Kafka writer namespace [%s] from checkpoint [%d]",
                            restoredWriterNamespace, restoredCheckpointId));
        }
        // The checkpoint component is shared by every state in one restore attempt and advances
        // before a relocated namespace can appear in a later checkpoint. A task retry therefore
        // derives the same namespace, while a later rescale cannot recreate a relocation that was
        // assigned to a sibling writer by that later checkpoint.
        return restoredWriterNamespace + "r" + currentWriterIndex + "c" + restoredCheckpointId;
    }

    /**
     * Fences every unfinished transaction namespace assigned to this writer by the engine's state
     * redistribution, plus this writer's current namespace. Transaction sequences are contiguous,
     * so probing until the first unused ID also covers transactions opened by a failed attempt
     * after its last completed checkpoint.
     */
    private void abortRestoredTransactions(
            KafkaTransactionSender<byte[], byte[]> transactionSender, RestorePlan restorePlan) {
        Map<String, Long> nextSequenceByPrefix = new LinkedHashMap<>();
        for (RestoredTransaction transaction : restorePlan.restoredTransactions) {
            nextSequenceByPrefix.merge(
                    transaction.transactionIdPrefix,
                    transaction.transactionSequence + 1,
                    Math::min);
        }
        nextSequenceByPrefix.merge(
                restorePlan.writerTransactionPrefix,
                restorePlan.nextTransactionSequence,
                Math::min);
        nextSequenceByPrefix.forEach(transactionSender::abortTransaction);
    }

    /**
     * Parses one checkpointed transaction before any Kafka producer is created.
     *
     * <p>The released legacy form is {@code <job-prefix>-<checkpoint>}. The current form is {@code
     * <job-prefix>-g<generation>w<writer>[r<adopting-writer>c<checkpoint>]-<sequence>}. Each state
     * is validated relative to its own checkpointed job prefix, which is required when historical
     * writers used different generated prefixes. Malformed IDs, foreign prefixes, and unsupported
     * writer namespaces fail restore before transaction fencing can have broker-side effects.
     */
    private RestoredTransaction parseRestoredTransaction(KafkaSinkState state) {
        String transactionId = state.getTransactionId();
        int transactionSequenceSeparator =
                transactionId == null ? -1 : transactionId.lastIndexOf('-');
        if (transactionSequenceSeparator < 0
                || transactionSequenceSeparator == transactionId.length() - 1) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot restore Kafka transaction [%s] for checkpoint [%d]",
                            transactionId, state.getCheckpointId()));
        }
        final long transactionSequence;
        try {
            transactionSequence =
                    Long.parseLong(transactionId.substring(transactionSequenceSeparator + 1));
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot restore Kafka transaction [%s] for checkpoint [%d]",
                            transactionId, state.getCheckpointId()),
                    e);
        }
        String restoredPrefix = transactionId.substring(0, transactionSequenceSeparator);
        String restoredTransactionPrefix = state.getTransactionIdPrefix();
        if (restoredTransactionPrefix == null
                || (!restoredPrefix.equals(restoredTransactionPrefix)
                        && !restoredPrefix.startsWith(restoredTransactionPrefix + "-"))) {
            throw new IllegalStateException(
                    String.format(
                            "Restored Kafka transaction [%s] does not belong to prefix [%s]",
                            transactionId, restoredTransactionPrefix));
        }
        boolean legacy = restoredPrefix.equals(restoredTransactionPrefix);
        String writerNamespace = null;
        if (!legacy) {
            writerNamespace = restoredPrefix.substring(restoredTransactionPrefix.length() + 1);
            if (!WRITER_NAMESPACE_PATTERN.matcher(writerNamespace).matches()) {
                throw new IllegalStateException(
                        String.format(
                                "Cannot determine the writer owner of Kafka transaction prefix [%s]",
                                restoredPrefix));
            }
        }
        return new RestoredTransaction(
                restoredTransactionPrefix,
                restoredPrefix,
                writerNamespace,
                transactionSequence,
                state.getCheckpointId(),
                legacy);
    }

    private static final class RestorePlan {
        private final String writerTransactionPrefix;
        private final long nextTransactionSequence;
        private final List<RestoredTransaction> restoredTransactions;

        private RestorePlan(
                String writerTransactionPrefix,
                long nextTransactionSequence,
                List<RestoredTransaction> restoredTransactions) {
            this.writerTransactionPrefix = writerTransactionPrefix;
            this.nextTransactionSequence = nextTransactionSequence;
            this.restoredTransactions = restoredTransactions;
        }
    }

    private static final class RestoredTransaction {
        private final String transactionPrefix;
        private final String transactionIdPrefix;
        private final String writerNamespace;
        /**
         * Contiguous trailing transaction-ID number used to resume and probe the writer namespace;
         * it is independent of the engine checkpoint ID.
         */
        private final long transactionSequence;
        /**
         * Engine checkpoint that produced this state; used to qualify deterministic relocation, not
         * to advance the transaction sequence.
         */
        private final long checkpointId;

        private final boolean legacy;

        private RestoredTransaction(
                String transactionPrefix,
                String transactionIdPrefix,
                String writerNamespace,
                long transactionSequence,
                long checkpointId,
                boolean legacy) {
            this.transactionPrefix = transactionPrefix;
            this.transactionIdPrefix = transactionIdPrefix;
            this.writerNamespace = writerNamespace;
            this.transactionSequence = transactionSequence;
            this.checkpointId = checkpointId;
            this.legacy = legacy;
        }
    }
}
