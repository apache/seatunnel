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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;

import static org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT;

/**
 * Builds the client settings shared by every MongoDB sink component that opens a connection (the
 * writer, the transaction committer, the save-mode catalog and the dry-run validator), so that the
 * optional {@code tls_ca_file} is applied consistently.
 *
 * <p>Without a CA file, clients are created exactly from the connection string, as before. With a
 * CA file, TLS is enabled and a per-client {@link SSLContext} trusts only the certificates of that
 * file; JVM-wide trust settings are neither used nor changed. All other settings, including {@code
 * tlsAllowInvalidHostnames}, still come from the connection string, so hostname verification stays
 * enabled by default.
 *
 * <p>Error messages never include the connection string, which can carry credentials.
 */
public final class MongodbClientSettingsFactory {

    private static final String TLS_CA_FILE_OPTION = "tls_ca_file";

    private MongodbClientSettingsFactory() {}

    /**
     * Creates a client for the given connection string, applying the CA file when one is set.
     *
     * @param connectionString MongoDB connection string
     * @param tlsCaFile optional path to a PEM CA bundle; {@code null} keeps the previous behavior
     */
    public static MongoClient createClient(String connectionString, String tlsCaFile) {
        if (tlsCaFile == null) {
            return MongoClients.create(connectionString);
        }
        return MongoClients.create(createSettings(connectionString, tlsCaFile));
    }

    /**
     * Creates client settings from the connection string, applying the CA file when one is set.
     *
     * @param connectionString MongoDB connection string
     * @param tlsCaFile optional path to a PEM CA bundle
     * @return settings that can be further customized by the caller
     */
    public static MongoClientSettings createSettings(String connectionString, String tlsCaFile) {
        ConnectionString parsed = new ConnectionString(connectionString);
        MongoClientSettings.Builder builder =
                MongoClientSettings.builder().applyConnectionString(parsed);
        if (tlsCaFile != null) {
            if (Boolean.FALSE.equals(parsed.getSslEnabled())) {
                throw new MongodbConnectorException(
                        ILLEGAL_ARGUMENT,
                        "MongoDB sink option '"
                                + TLS_CA_FILE_OPTION
                                + "' requires TLS, but the connection string disables it "
                                + "(tls=false or ssl=false).");
            }
            SSLContext sslContext = createSslContext(tlsCaFile);
            // Only enables TLS and replaces the trust material; the hostname verification
            // setting applied from the connection string above is kept.
            builder.applyToSslSettings(ssl -> ssl.enabled(true).context(sslContext));
        }
        return builder.build();
    }

    /** Builds an isolated trust context from every X.509 certificate in the CA bundle. */
    static SSLContext createSslContext(String tlsCaFile) {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] {createTrustManager(tlsCaFile)}, null);
            return sslContext;
        } catch (GeneralSecurityException e) {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT,
                    "Failed to create the TLS context for MongoDB sink option '"
                            + TLS_CA_FILE_OPTION
                            + "': "
                            + tlsCaFile,
                    e);
        }
    }

    static X509TrustManager createTrustManager(String tlsCaFile) {
        Path path = caFilePath(tlsCaFile);
        try (InputStream inputStream = Files.newInputStream(path)) {
            Collection<? extends Certificate> certificates =
                    CertificateFactory.getInstance("X.509").generateCertificates(inputStream);
            if (certificates.isEmpty()) {
                throw new MongodbConnectorException(
                        ILLEGAL_ARGUMENT,
                        "MongoDB sink option '"
                                + TLS_CA_FILE_OPTION
                                + "' contains no certificate: "
                                + tlsCaFile);
            }
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);
            int index = 0;
            for (Certificate certificate : certificates) {
                trustStore.setCertificateEntry("mongodb-ca-" + index++, certificate);
            }
            TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
                if (trustManager instanceof X509TrustManager) {
                    return (X509TrustManager) trustManager;
                }
            }
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT, "No X.509 trust manager available for " + tlsCaFile);
        } catch (IOException | GeneralSecurityException e) {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT,
                    "Failed to load the PEM CA bundle of MongoDB sink option '"
                            + TLS_CA_FILE_OPTION
                            + "': "
                            + tlsCaFile,
                    e);
        }
    }

    private static Path caFilePath(String tlsCaFile) {
        if (tlsCaFile.trim().isEmpty()) {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT,
                    "MongoDB sink option '" + TLS_CA_FILE_OPTION + "' must not be blank.");
        }
        Path path = Paths.get(tlsCaFile.trim());
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT,
                    "MongoDB sink option '"
                            + TLS_CA_FILE_OPTION
                            + "' is not a readable file: "
                            + tlsCaFile);
        }
        return path;
    }
}
