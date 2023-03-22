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

package org.apache.seatunnel.e2e.connector.google.firestore;

import org.apache.seatunnel.connectors.seatunnel.google.firestore.config.FirestoreConfig;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Base64;
import java.util.List;

@Disabled("Disabled because it needs google firestore database to run this test")
public class GoogleFirestoreIT extends TestSuiteBase implements TestResource {

    private static final String FIRESTORE_CONF_FILE = "/firestore/fake_to_google_firestore.conf";

    private String projectId;
    private String collection;
    private String credentials;
    private Firestore db;
    private CollectionReference collectionReference;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        initFirestoreConfig();
        FirestoreOptions firestoreOptions =
                FirestoreOptions.getDefaultInstance()
                        .toBuilder()
                        .setProjectId(projectId)
                        .setCredentials(
                                GoogleCredentials.fromStream(
                                        new ByteArrayInputStream(
                                                Base64.getDecoder().decode(credentials))))
                        .build();
        this.db = firestoreOptions.getService();
        this.collectionReference = db.collection(collection);
    }

    private void initFirestoreConfig() {
        File file = ContainerUtil.getResourcesFile(FIRESTORE_CONF_FILE);
        Config config = ConfigFactory.parseFile(file);
        Config firestoreConfig = config.getConfig("sink").getConfig("GoogleFirestore");
        this.projectId = firestoreConfig.getString(FirestoreConfig.PROJECT_ID.key());
        this.collection = firestoreConfig.getString(FirestoreConfig.COLLECTION.key());
        this.credentials = firestoreConfig.getString(FirestoreConfig.CREDENTIALS.key());
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (db != null) {
            db.close();
        }
    }

    @TestTemplate
    public void testGoogleFirestore(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob(FIRESTORE_CONF_FILE);
        Assertions.assertEquals(0, execResult.getExitCode());

        List<QueryDocumentSnapshot> documents = readSinkDataset();
        Assertions.assertTrue(documents.size() > 1);
        Assertions.assertEquals(15, documents.get(0).getData().size());
    }

    private List<QueryDocumentSnapshot> readSinkDataset() throws Exception {
        ApiFuture<QuerySnapshot> future = collectionReference.get();
        List<QueryDocumentSnapshot> documents = future.get().getDocuments();
        return documents;
    }
}
