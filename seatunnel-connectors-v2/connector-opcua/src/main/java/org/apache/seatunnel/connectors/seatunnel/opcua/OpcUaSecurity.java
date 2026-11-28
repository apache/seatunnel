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

package org.apache.seatunnel.connectors.seatunnel.opcua;

import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfigBuilder;
import org.eclipse.milo.opcua.stack.client.security.ClientCertificateValidator;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.util.CertificateUtil;
import org.eclipse.milo.opcua.stack.core.util.validation.CertificateValidationUtil;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;

/** The configured leaf certificate is the server trust anchor, with mandatory identity checks. */
final class OpcUaSecurity implements ClientCertificateValidator {
    private static final int MAX_KEY_STORE_BYTES = 4 * 1024 * 1024;
    private final OpcUaSettings settings;
    private final byte[] pin;
    private final String host;

    OpcUaSecurity(OpcUaSettings settings) {
        this.settings = settings;
        host = settings.endpoint.getHost().replace("[", "").replace("]", "");
        pin = new byte[32];
        for (int i = 0; i < pin.length; i++) {
            pin[i] = (byte) Integer.parseInt(settings.fingerprint.substring(i * 2, i * 2 + 2), 16);
        }
    }

    void configure(OpcUaClientConfigBuilder builder, EndpointDescription endpoint)
            throws OpcUaFailure {
        try {
            if (endpoint.getServer() == null
                    || !settings.serverUri.equals(endpoint.getServer().getApplicationUri())
                    || endpoint.getServerCertificate() == null
                    || endpoint.getServerCertificate().isNull()) {
                throw OpcUaFailure.problem(
                        "discovered server certificate or application URI does not match");
            }
            validateCertificateChain(
                    CertificateUtil.decodeCertificates(endpoint.getServerCertificate().bytes()));
        } catch (UaException e) {
            throw OpcUaFailure.problem(
                    "server certificate trust, hostname, validity or application URI check failed");
        }

        char[] password = settings.password.toCharArray();
        try {
            Path path = Path.of(settings.keyStore);
            if (!Files.isRegularFile(path)) {
                throw OpcUaFailure.problem("client_key_store must be a regular file");
            }
            KeyStore identity = KeyStore.getInstance("PKCS12");
            try (InputStream input = Files.newInputStream(path)) {
                byte[] encoded = input.readNBytes(MAX_KEY_STORE_BYTES + 1);
                try {
                    if (encoded.length > MAX_KEY_STORE_BYTES) {
                        throw OpcUaFailure.problem("client_key_store exceeds its size limit");
                    }
                    identity.load(new ByteArrayInputStream(encoded), password);
                } finally {
                    Arrays.fill(encoded, (byte) 0);
                }
            }
            PrivateKey key = (PrivateKey) identity.getKey(settings.keyAlias, password);
            X509Certificate certificate =
                    (X509Certificate) identity.getCertificate(settings.keyAlias);
            if (key == null || certificate == null) {
                throw OpcUaFailure.problem(
                        "client key alias does not contain a private key and certificate");
            }
            CertificateValidationUtil.checkValidity(certificate, true);
            CertificateValidationUtil.checkApplicationUri(certificate, settings.applicationUri);
            Certificate[] keyChain = identity.getCertificateChain(settings.keyAlias);
            if (keyChain == null || keyChain.length == 0) {
                throw OpcUaFailure.problem("client key alias has no certificate chain");
            }
            X509Certificate[] chain =
                    Arrays.copyOf(keyChain, keyChain.length, X509Certificate[].class);
            builder.setKeyPair(new KeyPair(certificate.getPublicKey(), key));
            builder.setCertificate(certificate);
            builder.setCertificateChain(chain);
            builder.setCertificateValidator(this);
        } catch (OpcUaFailure e) {
            throw e;
        } catch (Exception e) {
            throw OpcUaFailure.problem("could not load or validate the PKCS12 client identity");
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    @Override
    public void validateCertificateChain(List<X509Certificate> certificates) throws UaException {
        if (certificates.isEmpty()) {
            throw new UaException(StatusCodes.Bad_CertificateInvalid);
        }
        X509Certificate certificate = certificates.get(0);
        try {
            if (!MessageDigest.isEqual(
                    pin, MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()))) {
                throw new UaException(StatusCodes.Bad_CertificateUntrusted);
            }
        } catch (GeneralSecurityException e) {
            throw new UaException(StatusCodes.Bad_CertificateInvalid);
        }
        CertificateValidationUtil.checkValidity(certificate, true);
        CertificateValidationUtil.checkHostnameOrIpAddress(certificate, host);
        CertificateValidationUtil.checkApplicationUri(certificate, settings.serverUri);
    }

    @Override
    public void validateCertificateChain(
            List<X509Certificate> certificates, String uri, String... hosts) throws UaException {
        if (!settings.serverUri.equals(uri)) {
            throw new UaException(StatusCodes.Bad_CertificateUriInvalid);
        }
        validateCertificateChain(certificates);
    }
}
