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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Base64;

/**
 * Test-only TLS material from {@code src/test/resources/tls} (see {@code generate-test-certs.sh}):
 * a test CA, an unrelated CA, and two server key stores signed by the test CA, one for {@code
 * DNS:localhost} and one for {@code DNS:unrelated-host.invalid} only.
 */
public final class TlsTestMaterial {

    private static final char[] PASSWORD = "changeit".toCharArray();

    private TlsTestMaterial() {}

    /** The CA that signed the test server certificate. */
    public static X509Certificate testCa() {
        return caCertificate("tls/test-ca.p12");
    }

    /** A CA that did not sign the test server certificate. */
    public static X509Certificate untrustedCa() {
        return caCertificate("tls/untrusted-ca.p12");
    }

    /** Writes the certificates as a PEM bundle, the format expected by {@code tls_ca_file}. */
    public static Path writePem(Path directory, String fileName, X509Certificate... certificates)
            throws Exception {
        StringBuilder pem = new StringBuilder();
        Base64.Encoder encoder =
                Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII));
        for (X509Certificate certificate : certificates) {
            pem.append("-----BEGIN CERTIFICATE-----\n")
                    .append(encoder.encodeToString(certificate.getEncoded()))
                    .append("\n-----END CERTIFICATE-----\n");
        }
        Path file = directory.resolve(fileName);
        Files.write(file, pem.toString().getBytes(StandardCharsets.US_ASCII));
        return file;
    }

    /** Server-side TLS context presenting the certificate for {@code DNS:localhost}. */
    public static SSLContext localhostServerContext() throws Exception {
        return serverContext("tls/server.p12");
    }

    /** Server-side TLS context whose certificate does not cover {@code localhost}. */
    public static SSLContext wrongHostServerContext() throws Exception {
        return serverContext("tls/wrong-host-server.p12");
    }

    private static SSLContext serverContext(String resource) throws Exception {
        KeyManagerFactory keyManagerFactory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(load(resource), PASSWORD);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagerFactory.getKeyManagers(), null, null);
        return context;
    }

    private static X509Certificate caCertificate(String resource) {
        try {
            return (X509Certificate) load(resource).getCertificate("ca");
        } catch (Exception e) {
            throw new IllegalStateException("Cannot load test certificate " + resource, e);
        }
    }

    private static KeyStore load(String resource) throws Exception {
        try (InputStream in =
                TlsTestMaterial.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("Missing test resource " + resource);
            }
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(in, PASSWORD);
            return keyStore;
        }
    }
}
