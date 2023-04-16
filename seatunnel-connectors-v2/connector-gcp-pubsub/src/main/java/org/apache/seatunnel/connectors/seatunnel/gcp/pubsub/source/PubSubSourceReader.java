/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.gcp.pubsub.source;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.gcp.pubsub.config.PubSubParameters;
import org.apache.seatunnel.connectors.seatunnel.gcp.pubsub.deserialize.PubSubDeserializer;
import org.apache.seatunnel.connectors.seatunnel.gcp.pubsub.deserialize.SeaTunnelRowDeserializer;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;

@Slf4j
public class PubSubSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    private final PubSubParameters parameters;

    private final SeaTunnelRowDeserializer deserializer;

    private ProjectSubscriptionName subscriptionName;

    private CredentialsProvider credentialsProvider;

    public PubSubSourceReader(PubSubParameters parameters, SeaTunnelRowType rowType, DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.parameters = parameters;
        this.deserializer = new PubSubDeserializer(rowType.getFieldNames(), deserializationSchema);
    }

    @Override
    public void open() throws Exception {
        subscriptionName = ProjectSubscriptionName.of(parameters.getProjectId(), parameters.getSubscriptionId());
        ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(Files.newInputStream(new File(parameters.getServiceAccountKey()).toPath()));
        // Create a credentials provider
        credentialsProvider = FixedCredentialsProvider.create(credentials);
        log.info("Listening for messages on {}: \n", subscriptionName.getSubscription());
    }

    @Override
    public void close() {
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        Subscriber subscriber = Subscriber.newBuilder(subscriptionName, new SeaTunnelMessageReceiver(deserializer, output)).setCredentialsProvider(credentialsProvider).build();

        // start the subscriber
        subscriber.startAsync().awaitRunning();
    }

    private static final class SeaTunnelMessageReceiver implements MessageReceiver {

        private final SeaTunnelRowDeserializer deserializer;
        private final Collector<SeaTunnelRow> output;

        public SeaTunnelMessageReceiver(SeaTunnelRowDeserializer deserializer, Collector<SeaTunnelRow> output) {
            this.deserializer = deserializer;
            this.output = output;
        }

        @Override
        public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
            ByteString data = pubsubMessage.getData();
            if (data.isEmpty()) {
                ackReplyConsumer.nack();
            } else {
                String value = data.toStringUtf8();
                SeaTunnelRow seaTunnelRow = this.deserializer.deserializeRow(Collections.singletonList(value));
                output.collect(seaTunnelRow);
                ackReplyConsumer.ack();
            }
        }
    }
}
