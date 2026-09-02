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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.mongodb.MongoClientSettings;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class AmazonDocumentDBConfigTest {

    @TempDir private Path tempDirectory;

    @Test
    public void testForcesRetryWritesFalse() {
        AmazonDocumentDBConfig config =
                new AmazonDocumentDBConfig(ReadonlyConfig.fromMap(baseOptions()));

        MongoClientSettings settings = config.createMongoClientSettings();

        Assertions.assertFalse(settings.getRetryWrites());
        Assertions.assertFalse(settings.getSslSettings().isEnabled());
        Assertions.assertEquals("app-db", config.getDatabase());
        Assertions.assertEquals("orders", config.getCollection());
    }

    @Test
    public void testRejectsRetryWritesTrue() {
        Map<String, Object> options = baseOptions();
        options.put(
                "uri",
                "mongodb://reader:secret@cluster.example.docdb.amazonaws.com:27017/?retryWrites=true");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new AmazonDocumentDBConfig(ReadonlyConfig.fromMap(options)));

        Assertions.assertTrue(exception.getMessage().contains("retryWrites=true"));
        Assertions.assertTrue(exception.getMessage().contains("does not support retryable writes"));
    }

    @Test
    public void testRequiresCredentialsInUri() {
        Map<String, Object> options = baseOptions();
        options.put("uri", "mongodb://cluster.example.docdb.amazonaws.com:27017/");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new AmazonDocumentDBConfig(ReadonlyConfig.fromMap(options)));

        Assertions.assertTrue(exception.getMessage().contains("authentication credentials"));
    }

    @Test
    public void testTlsRequiresReadableCaBundle() throws Exception {
        Map<String, Object> missingCaOptions = baseOptions();
        missingCaOptions.put("tls", true);

        IllegalArgumentException missingCaException =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new AmazonDocumentDBConfig(ReadonlyConfig.fromMap(missingCaOptions)));
        Assertions.assertTrue(missingCaException.getMessage().contains("tls_ca_file"));

        Path invalidBundle = tempDirectory.resolve("invalid-ca.pem");
        Files.write(invalidBundle, "not a certificate".getBytes());
        Map<String, Object> invalidCaOptions = baseOptions();
        invalidCaOptions.put("tls", true);
        invalidCaOptions.put("tls_ca_file", invalidBundle.toString());
        AmazonDocumentDBConfig config =
                new AmazonDocumentDBConfig(ReadonlyConfig.fromMap(invalidCaOptions));

        IllegalArgumentException invalidCaException =
                Assertions.assertThrows(
                        IllegalArgumentException.class, config::createMongoClientSettings);
        Assertions.assertTrue(invalidCaException.getMessage().contains("TLS CA bundle"));
    }

    @Test
    public void testRejectsInvalidFilter() {
        Map<String, Object> options = baseOptions();
        options.put("match.query", "{invalid");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new AmazonDocumentDBConfig(ReadonlyConfig.fromMap(options)));

        Assertions.assertTrue(exception.getMessage().contains("match.query"));
    }

    private static Map<String, Object> baseOptions() {
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", new HashMap<String, Object>());

        Map<String, Object> options = new HashMap<>();
        options.put(
                "uri",
                "mongodb://reader:secret@cluster.example.docdb.amazonaws.com:27017/?retryWrites=false");
        options.put("database", "app-db");
        options.put("collection", "orders");
        options.put("tls", false);
        options.put("match.query", "{\"status\": \"OPEN\"}");
        options.put("match.projection", "{\"_id\": 1, \"status\": 1}");
        options.put("fetch.size", 128);
        options.put("schema", schema);
        return options;
    }
}
