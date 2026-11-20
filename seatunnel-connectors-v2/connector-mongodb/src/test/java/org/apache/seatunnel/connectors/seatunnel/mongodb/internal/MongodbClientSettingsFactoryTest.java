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

package org.apache.seatunnel.connectors.seatunnel.mongodb.internal;

import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.SslSettings;

import javax.net.ssl.X509TrustManager;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

class MongodbClientSettingsFactoryTest {

    private static final String SECRET_URI = "mongodb://user:secret@localhost:27017";

    @TempDir Path tempDir;

    @Test
    void withoutCaFileKeepsConnectionStringTlsSettings() {
        SslSettings plain = ssl(MongodbClientSettingsFactory.createSettings(SECRET_URI, null));
        Assertions.assertFalse(plain.isEnabled());
        Assertions.assertNull(plain.getContext());

        SslSettings tls =
                ssl(MongodbClientSettingsFactory.createSettings(SECRET_URI + "/?tls=true", null));
        Assertions.assertTrue(tls.isEnabled());
        Assertions.assertNull(tls.getContext(), "JVM default trust must stay in use");
    }

    @Test
    void withoutCaFileCreatesClientFromConnectionStringAsBefore() {
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            MongodbClientSettingsFactory.createClient(SECRET_URI, null);
            clients.verify(() -> MongoClients.create(SECRET_URI));
            clients.verify(() -> MongoClients.create(any(MongoClientSettings.class)), never());
        }
    }

    @Test
    void caFileEnablesTlsWithIsolatedTrustContext() throws Exception {
        Path ca = TlsTestMaterial.writePem(tempDir, "ca.pem", TlsTestMaterial.testCa());

        SslSettings ssl =
                ssl(MongodbClientSettingsFactory.createSettings(SECRET_URI, ca.toString()));

        Assertions.assertTrue(ssl.isEnabled(), "setting tls_ca_file enables TLS");
        Assertions.assertNotNull(ssl.getContext());
        Assertions.assertFalse(ssl.isInvalidHostNameAllowed(), "hostname verification stays on");
    }

    @Test
    void caFileKeepsHostnameSettingFromConnectionString() throws Exception {
        Path ca = TlsTestMaterial.writePem(tempDir, "ca.pem", TlsTestMaterial.testCa());

        SslSettings ssl =
                ssl(
                        MongodbClientSettingsFactory.createSettings(
                                SECRET_URI + "/?tls=true&tlsAllowInvalidHostnames=true",
                                ca.toString()));

        Assertions.assertNotNull(ssl.getContext());
        Assertions.assertTrue(ssl.isInvalidHostNameAllowed());
    }

    @Test
    void caFileCreatesClientFromSettings() throws Exception {
        Path ca = TlsTestMaterial.writePem(tempDir, "ca.pem", TlsTestMaterial.testCa());
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            MongodbClientSettingsFactory.createClient(SECRET_URI, ca.toString());
            clients.verify(() -> MongoClients.create(any(MongoClientSettings.class)));
            clients.verify(() -> MongoClients.create(anyString()), never());
        }
    }

    @Test
    void trustsEveryCertificateOfTheBundle() throws Exception {
        Path bundle =
                TlsTestMaterial.writePem(
                        tempDir,
                        "bundle.pem",
                        TlsTestMaterial.testCa(),
                        TlsTestMaterial.untrustedCa());

        X509TrustManager trustManager =
                MongodbClientSettingsFactory.createTrustManager(bundle.toString());

        Set<String> issuers =
                Arrays.stream(trustManager.getAcceptedIssuers())
                        .map(certificate -> certificate.getSubjectX500Principal().getName())
                        .collect(Collectors.toSet());
        Assertions.assertEquals(2, issuers.size());
        Assertions.assertTrue(issuers.contains("CN=SeaTunnel MongoDB Test CA"));
        Assertions.assertTrue(issuers.contains("CN=SeaTunnel MongoDB Untrusted Test CA"));
    }

    @Test
    void rejectsCaFileWhenConnectionStringDisablesTls() throws Exception {
        Path ca = TlsTestMaterial.writePem(tempDir, "ca.pem", TlsTestMaterial.testCa());

        assertConfigError(
                () ->
                        MongodbClientSettingsFactory.createSettings(
                                SECRET_URI + "/?tls=false", ca.toString()),
                "requires TLS");
    }

    @Test
    void rejectsMissingBlankAndInvalidCaFiles() throws Exception {
        Path missing = tempDir.resolve("missing.pem");
        Path noCertificate = tempDir.resolve("empty.pem");
        Files.write(noCertificate, new byte[0]);
        Path garbage = tempDir.resolve("garbage.pem");
        Files.write(
                garbage,
                "-----BEGIN CERTIFICATE-----\nnot a certificate\n-----END CERTIFICATE-----\n"
                        .getBytes(StandardCharsets.US_ASCII));

        assertConfigError(
                () -> MongodbClientSettingsFactory.createSettings(SECRET_URI, missing.toString()),
                "is not a readable file: " + missing);
        assertConfigError(
                () -> MongodbClientSettingsFactory.createSettings(SECRET_URI, tempDir.toString()),
                "is not a readable file");
        assertConfigError(
                () -> MongodbClientSettingsFactory.createSettings(SECRET_URI, "  "),
                "must not be blank");
        assertConfigError(
                () ->
                        MongodbClientSettingsFactory.createSettings(
                                SECRET_URI, noCertificate.toString()),
                noCertificate.toString());
        assertConfigError(
                () -> MongodbClientSettingsFactory.createSettings(SECRET_URI, garbage.toString()),
                garbage.toString());
    }

    private static SslSettings ssl(MongoClientSettings settings) {
        return settings.getSslSettings();
    }

    private static void assertConfigError(Runnable action, String expectedMessagePart) {
        MongodbConnectorException e =
                Assertions.assertThrows(MongodbConnectorException.class, action::run);
        Assertions.assertTrue(
                e.getMessage().contains("tls_ca_file"), "names the option: " + e.getMessage());
        Assertions.assertTrue(
                e.getMessage().contains(expectedMessagePart),
                "expected '" + expectedMessagePart + "' in: " + e.getMessage());
        Assertions.assertFalse(
                e.getMessage().contains("secret"), "must not leak the URI: " + e.getMessage());
    }
}
