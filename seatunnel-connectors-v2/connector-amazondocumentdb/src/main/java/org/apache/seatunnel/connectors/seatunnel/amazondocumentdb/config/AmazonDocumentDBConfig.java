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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import org.bson.BsonDocument;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;

/**
 * Validated runtime configuration for the Amazon DocumentDB source.
 *
 * <p>The configuration rejects unsupported retryable writes before a reader starts and owns all TLS
 * trust material so one job cannot change the JVM-wide trust configuration of another job.
 */
public class AmazonDocumentDBConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String uri;
    private final String database;
    private final String collection;
    private final boolean tls;
    private final String tlsCaFile;
    private final String matchQuery;
    private final String projection;
    private final int fetchSize;
    private final Config schema;

    public AmazonDocumentDBConfig(ReadonlyConfig config) {
        this.uri = requireNonBlank(config.get(AmazonDocumentDBSourceOptions.URI), "uri");
        this.database =
                requireNonBlank(config.get(AmazonDocumentDBSourceOptions.DATABASE), "database");
        this.collection =
                requireNonBlank(config.get(AmazonDocumentDBSourceOptions.COLLECTION), "collection");
        this.tls = config.get(AmazonDocumentDBSourceOptions.TLS);
        this.tlsCaFile =
                config.getOptional(AmazonDocumentDBSourceOptions.TLS_CA_FILE)
                        .map(String::trim)
                        .filter(value -> !value.isEmpty())
                        .orElse(null);
        this.matchQuery = config.get(AmazonDocumentDBSourceOptions.MATCH_QUERY);
        this.projection =
                config.getOptional(AmazonDocumentDBSourceOptions.PROJECTION)
                        .map(String::trim)
                        .filter(value -> !value.isEmpty())
                        .orElse(null);
        this.fetchSize = config.get(AmazonDocumentDBSourceOptions.FETCH_SIZE);
        this.schema =
                config.getOptional(ConnectorCommonOptions.SCHEMA)
                        .map(ReadonlyConfig::fromMap)
                        .map(ReadonlyConfig::toConfig)
                        .orElse(null);

        ConnectionString connectionString = parseConnectionString(uri);
        if (connectionString.getCredential() == null) {
            throw new IllegalArgumentException(
                    "AmazonDocumentDB option 'uri' must include authentication credentials");
        }
        if (hasRetryWritesEnabled(uri)) {
            throw new IllegalArgumentException(
                    "AmazonDocumentDB does not support retryable writes; remove 'retryWrites=true' from option 'uri' or set it to false");
        }
        if (tls) {
            validateTlsCaFile(tlsCaFile);
        }
        validateBsonDocument(matchQuery, "match.query");
        if (projection != null) {
            validateBsonDocument(projection, "match.projection");
        }
    }

    /**
     * Builds driver settings with DocumentDB-safe overrides.
     *
     * <p>The URI is applied first and {@code retryWrites(false)} second deliberately: the latter
     * must win over both driver defaults and any URI option. TLS uses a connector-local {@link
     * SSLContext} built from the configured CA bundle instead of mutating the JVM-global trust
     * store, which would affect unrelated connectors running in the same process.
     */
    public MongoClientSettings createMongoClientSettings() {
        ConnectionString connectionString = parseConnectionString(uri);
        MongoClientSettings.Builder builder =
                MongoClientSettings.builder()
                        .applyConnectionString(connectionString)
                        .retryWrites(false);
        builder.applyToSslSettings(
                sslBuilder -> {
                    sslBuilder.enabled(tls);
                    if (tls) {
                        sslBuilder.context(createSslContext(Paths.get(tlsCaFile)));
                    }
                });
        return builder.build();
    }

    private static ConnectionString parseConnectionString(String uri) {
        try {
            return new ConnectionString(uri);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid AmazonDocumentDB connection URI in option 'uri'", e);
        }
    }

    private static boolean hasRetryWritesEnabled(String uri) {
        int queryStart = uri.indexOf('?');
        if (queryStart < 0 || queryStart == uri.length() - 1) {
            return false;
        }
        String query = uri.substring(queryStart + 1);
        int fragmentStart = query.indexOf('#');
        if (fragmentStart >= 0) {
            query = query.substring(0, fragmentStart);
        }
        for (String parameter : query.split("&")) {
            int separator = parameter.indexOf('=');
            if (separator < 0) {
                continue;
            }
            String key = decodeUriParameter(parameter.substring(0, separator));
            String value = decodeUriParameter(parameter.substring(separator + 1));
            if ("retryWrites".equalsIgnoreCase(key) && "true".equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }

    private static String decodeUriParameter(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 must be supported", e);
        }
    }

    private static String requireNonBlank(String value, String optionName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "AmazonDocumentDB option '" + optionName + "' must not be blank");
        }
        return value.trim();
    }

    private static void validateTlsCaFile(String tlsCaFile) {
        if (tlsCaFile == null) {
            throw new IllegalArgumentException(
                    "AmazonDocumentDB option 'tls_ca_file' is required when TLS is enabled");
        }
        Path path = Paths.get(tlsCaFile);
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException(
                    "AmazonDocumentDB TLS CA bundle is not a readable file: " + tlsCaFile);
        }
    }

    private static void validateBsonDocument(String value, String optionName) {
        try {
            BsonDocument.parse(value);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "AmazonDocumentDB option '"
                            + optionName
                            + "' must be a valid BSON/JSON document",
                    e);
        }
    }

    /** Builds an isolated trust context from every X.509 certificate in the supplied CA bundle. */
    private static SSLContext createSslContext(Path caBundlePath) {
        try (InputStream inputStream = Files.newInputStream(caBundlePath)) {
            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            Collection<? extends Certificate> certificates =
                    certificateFactory.generateCertificates(inputStream);
            if (certificates.isEmpty()) {
                throw new IllegalArgumentException(
                        "AmazonDocumentDB TLS CA bundle contains no certificates: " + caBundlePath);
            }

            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);
            int certificateIndex = 0;
            for (Certificate certificate : certificates) {
                trustStore.setCertificateEntry(
                        "amazondocumentdb-ca-" + certificateIndex, certificate);
                certificateIndex++;
            }

            TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
            return sslContext;
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalArgumentException(
                    "Failed to load AmazonDocumentDB TLS CA bundle: " + caBundlePath, e);
        }
    }

    public String getUri() {
        return uri;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public boolean isTls() {
        return tls;
    }

    public String getTlsCaFile() {
        return tlsCaFile;
    }

    public String getMatchQuery() {
        return matchQuery;
    }

    public String getProjection() {
        return projection;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public Config getSchema() {
        return schema;
    }
}
