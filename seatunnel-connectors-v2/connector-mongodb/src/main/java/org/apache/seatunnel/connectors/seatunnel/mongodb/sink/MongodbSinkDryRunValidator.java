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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbSinkOptions;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import java.util.concurrent.TimeUnit;

final class MongodbSinkDryRunValidator {

    private static final long TIMEOUT_MS = 30_000L;

    private MongodbSinkDryRunValidator() {}

    /**
     * Connects using the configured credentials and sends only ping. This does not prove write
     * permissions, collection existence, schema compatibility, or transaction support.
     */
    static void validate(ReadonlyConfig options) {
        if (Thread.currentThread().isInterrupted()) {
            throw new IllegalStateException("MongoDB sink dry-run was interrupted.");
        }
        try (MongoClient client =
                MongoClients.create(settings(options.get(MongodbSinkOptions.URI)))) {
            // Writes select the primary regardless of the URI's read preference.
            client.getDatabase(options.get(MongodbSinkOptions.DATABASE))
                    .runCommand(new Document("ping", 1), ReadPreference.primary());
        } catch (MongoInterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("MongoDB sink dry-run was interrupted.");
        } catch (MongoSecurityException e) {
            throw new IllegalStateException("MongoDB sink dry-run authentication failed.");
        } catch (MongoTimeoutException e) {
            throw new IllegalStateException("MongoDB sink dry-run connection timed out.");
        } catch (RuntimeException e) {
            // Driver messages, causes and suppressed close failures can contain URI credentials.
            throw new IllegalStateException(
                    "MongoDB sink dry-run connection failed. Check the URI, network and TLS settings.");
        }
    }

    /** Preserves URI settings, but bounds individual waits; this is not a total deadline. */
    private static MongoClientSettings settings(String uri) {
        MongoClientSettings configured =
                MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(uri))
                        .build();
        long selectionTimeout =
                bounded(
                        configured
                                .getClusterSettings()
                                .getServerSelectionTimeout(TimeUnit.MILLISECONDS));
        int connectTimeout =
                (int)
                        bounded(
                                configured
                                        .getSocketSettings()
                                        .getConnectTimeout(TimeUnit.MILLISECONDS));
        int readTimeout =
                (int) bounded(configured.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
        long poolWaitTimeout =
                bounded(
                        configured
                                .getConnectionPoolSettings()
                                .getMaxWaitTime(TimeUnit.MILLISECONDS));
        return MongoClientSettings.builder(configured)
                .applyToClusterSettings(
                        builder ->
                                builder.serverSelectionTimeout(
                                        selectionTimeout, TimeUnit.MILLISECONDS))
                .applyToSocketSettings(
                        builder ->
                                builder.connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
                                        .readTimeout(readTimeout, TimeUnit.MILLISECONDS))
                .applyToConnectionPoolSettings(
                        builder ->
                                builder.minSize(0)
                                        .maxSize(1)
                                        .maxWaitTime(poolWaitTimeout, TimeUnit.MILLISECONDS))
                .build();
    }

    private static long bounded(long timeout) {
        return timeout > 0 ? Math.min(timeout, TIMEOUT_MS) : TIMEOUT_MS;
    }
}
