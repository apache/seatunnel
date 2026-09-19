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

package org.apache.seatunnel.connectors.seatunnel.mongodb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbSourceOptions;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Metadata-only checks, isolated from source readers and split enumeration. */
final class MongodbSourceDryRunValidator {
    private static final int MAX_TIMEOUT_MS = 30_000;

    private MongodbSourceDryRunValidator() {}

    static void validate(ReadonlyConfig options) throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("MongoDB connect dry-run interrupted");
        }
        try {
            MongoClientSettings settings = settings(options.get(MongodbSourceOptions.URI));
            try (MongoClient client = MongoClients.create(settings)) {
                MongoDatabase database =
                        client.getDatabase(options.get(MongodbSourceOptions.DATABASE));
                String collection = options.get(MongodbSourceOptions.COLLECTION);
                // Driver 4.7's listCollections API cannot set these two options. Together they
                // allow a collection-scoped user to inspect only its authorized namespaces.
                Document command =
                        new Document("listCollections", 1)
                                .append("filter", new Document("name", collection))
                                .append("nameOnly", true)
                                .append("authorizedCollections", true)
                                .append("cursor", new Document("batchSize", 2))
                                .append("maxTimeMS", MAX_TIMEOUT_MS);
                Document cursor =
                        database.runCommand(command, settings.getReadPreference())
                                .get("cursor", Document.class);
                try {
                    List<Document> batch = cursor.getList("firstBatch", Document.class);
                    if (batch.stream().noneMatch(row -> collection.equals(row.getString("name")))) {
                        throw new IllegalStateException(
                                "Configured MongoDB collection does not exist or is not visible to the configured user");
                    }
                } finally {
                    // The exact-name filter returns at most one result. Defensively release any
                    // cursor left open by the server instead of fetching additional metadata.
                    long cursorId = ((Number) cursor.get("id")).longValue();
                    if (cursorId != 0) {
                        database.runCommand(
                                new Document("killCursors", "$cmd.listCollections")
                                        .append("cursors", Collections.singletonList(cursorId)),
                                settings.getReadPreference());
                    }
                }
            }
        } catch (MongoInterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedException("MongoDB connect dry-run interrupted");
        } catch (MongoSecurityException e) {
            throw new IllegalStateException("MongoDB connect dry-run authentication failed");
        } catch (MongoTimeoutException e) {
            throw new IllegalStateException("MongoDB connect dry-run connection timed out");
        } catch (RuntimeException e) {
            // Driver exceptions can contain URI credentials, database names or server responses.
            // Do not retain their message, cause or suppressed exceptions in the CLI failure.
            throw new IllegalStateException(
                    "MongoDB connect dry-run could not validate the configured collection. Check the URI, database, collection and metadata permissions.");
        }
    }

    private static MongoClientSettings settings(String uri) {
        MongoClientSettings original =
                MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(uri))
                        .build();
        return MongoClientSettings.builder(original)
                .applyToClusterSettings(
                        builder ->
                                builder.serverSelectionTimeout(
                                        bounded(
                                                original.getClusterSettings()
                                                        .getServerSelectionTimeout(
                                                                TimeUnit.MILLISECONDS)),
                                        TimeUnit.MILLISECONDS))
                .applyToSocketSettings(
                        builder ->
                                builder.connectTimeout(
                                                bounded(
                                                        original.getSocketSettings()
                                                                .getConnectTimeout(
                                                                        TimeUnit.MILLISECONDS)),
                                                TimeUnit.MILLISECONDS)
                                        .readTimeout(
                                                bounded(
                                                        original.getSocketSettings()
                                                                .getReadTimeout(
                                                                        TimeUnit.MILLISECONDS)),
                                                TimeUnit.MILLISECONDS))
                .applyToConnectionPoolSettings(
                        builder ->
                                builder.minSize(0)
                                        .maxSize(1)
                                        .maxWaitTime(
                                                bounded(
                                                        original.getConnectionPoolSettings()
                                                                .getMaxWaitTime(
                                                                        TimeUnit.MILLISECONDS)),
                                                TimeUnit.MILLISECONDS))
                .build();
    }

    private static int bounded(long timeout) {
        return timeout <= 0 ? MAX_TIMEOUT_MS : (int) Math.min(timeout, MAX_TIMEOUT_MS);
    }
}
