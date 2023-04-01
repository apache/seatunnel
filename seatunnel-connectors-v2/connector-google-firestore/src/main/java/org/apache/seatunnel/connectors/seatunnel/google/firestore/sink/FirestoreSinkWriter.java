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

package org.apache.seatunnel.connectors.seatunnel.google.firestore.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.google.firestore.config.FirestoreParameters;
import org.apache.seatunnel.connectors.seatunnel.google.firestore.exception.FirestoreConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.google.firestore.exception.FirestoreConnectorException;
import org.apache.seatunnel.connectors.seatunnel.google.firestore.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.google.firestore.serialize.SeaTunnelRowSerializer;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;

@Slf4j
public class FirestoreSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private Firestore firestore;

    private CollectionReference collectionReference;

    private SeaTunnelRowSerializer serializer;

    public FirestoreSinkWriter(SeaTunnelRowType seaTunnelRowType, FirestoreParameters parameters)
            throws IOException {
        GoogleCredentials credentials;
        if (parameters.getCredentials() != null) {
            byte[] bytes = Base64.getDecoder().decode(parameters.getCredentials());
            credentials = GoogleCredentials.fromStream(new ByteArrayInputStream(bytes));
        } else {
            credentials = GoogleCredentials.getApplicationDefault();
        }
        FirestoreOptions firestoreOptions =
                FirestoreOptions.getDefaultInstance()
                        .toBuilder()
                        .setProjectId(parameters.getProjectId())
                        .setCredentials(credentials)
                        .build();
        this.firestore = firestoreOptions.getService();
        this.collectionReference = firestore.collection(parameters.getCollection());
        this.serializer = new DefaultSeaTunnelRowSerializer(seaTunnelRowType);
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) throws IOException {
        collectionReference.add(serializer.serialize(seaTunnelRow));
    }

    @Override
    public void close() throws IOException {
        if (firestore != null) {
            try {
                firestore.close();
            } catch (Exception e) {
                throw new FirestoreConnectorException(
                        FirestoreConnectorErrorCode.CLOSE_CLIENT_FAILED,
                        "Close Firestore client failed.",
                        e);
            }
        }
    }
}
