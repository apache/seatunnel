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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source.reader;

import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConsumerConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.start.StartCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop.StopCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class PulsarSplitReaderThread extends Thread implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSplitReaderThread.class);
    protected final PulsarSourceReader sourceReader;
    protected final PulsarPartitionSplit split;
    protected final PulsarClient pulsarClient;
    protected final PulsarConsumerConfig consumerConfig;
    /** The maximum number of milliseconds to wait for a fetch batch. */
    protected final int pollTimeout;

    protected final long pollInterval;
    protected final StartCursor startCursor;
    protected final Handover<RecordWithSplitId> handover;
    protected Consumer<byte[]> consumer;

    /** Flag to mark the main work loop as alive. */
    private volatile boolean running;

    public PulsarSplitReaderThread(
            PulsarSourceReader sourceReader,
            PulsarPartitionSplit split,
            PulsarClient pulsarClient,
            PulsarConsumerConfig consumerConfig,
            int pollTimeout,
            long pollInterval,
            StartCursor startCursor,
            Handover<RecordWithSplitId> handover) {
        this.sourceReader = sourceReader;
        this.split = split;
        this.pulsarClient = pulsarClient;
        this.consumerConfig = consumerConfig;
        this.pollTimeout = pollTimeout;
        this.pollInterval = pollInterval;
        this.startCursor = startCursor;
        this.handover = handover;
    }

    public void open() throws PulsarClientException {
        this.consumer = createPulsarConsumer(split);
        if (split.getLatestConsumedId() == null) {
            startCursor.seekPosition(consumer);
        }
        this.running = true;
    }

    @Override
    public void run() {
        try {
            final StopCursor stopCursor = split.getStopCursor();
            while (running) {
                Message<byte[]> message = consumer.receive(pollTimeout, TimeUnit.MILLISECONDS);
                if (message != null) {
                    handover.produce(new RecordWithSplitId(message, split.splitId()));
                    if (stopCursor.shouldStop(message)) {
                        sourceReader.handleNoMoreElements(split.splitId(), message.getMessageId());
                        break;
                    }
                }
                Thread.sleep(pollInterval);
            }
        } catch (Throwable t) {
            LOG.error("Pulsar Consumer receive data error", t);
            handover.reportError(t);
        } finally {
            // make sure the PulsarConsumer is closed
            try {
                consumer.close();
            } catch (Throwable t) {
                LOG.warn("Error while closing pulsar consumer", t);
            } finally {
                running = false;
            }
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (consumer != null) {
            consumer.close();
        }
    }

    public void committingCursor(MessageId offsetsToCommit) throws PulsarClientException {
        if (consumer == null) {
            consumer = createPulsarConsumer(split);
        }
        consumer.acknowledgeCumulative(offsetsToCommit);
    }

    /** Create a specified {@link Consumer} by the given split information. */
    protected Consumer<byte[]> createPulsarConsumer(PulsarPartitionSplit split) {
        ConsumerBuilder<byte[]> consumerBuilder =
                PulsarConfigUtil.createConsumerBuilder(pulsarClient, consumerConfig);

        consumerBuilder.topic(split.getPartition().getFullTopicName());

        // Create the consumer configuration by using common utils.
        try {
            return consumerBuilder.subscribe();
        } catch (PulsarClientException e) {
            throw new PulsarConnectorException(
                    PulsarConnectorErrorCode.OPEN_PULSAR_ADMIN_FAILED,
                    "Failed to create pulsar consumer:",
                    e);
        }
    }
}
