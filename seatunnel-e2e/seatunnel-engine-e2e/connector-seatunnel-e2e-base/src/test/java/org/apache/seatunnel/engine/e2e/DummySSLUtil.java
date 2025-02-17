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
package org.apache.seatunnel.engine.e2e;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Date;

public class DummySSLUtil {

    private static final char[] STORE_PASSWORD = "changeit".toCharArray();

    // Generates a self-signed certificate using BouncyCastle.
    public static X509Certificate generateSelfSignedCertificate(KeyPair keyPair) throws Exception {
        long now = System.currentTimeMillis();
        Date startDate = new Date(now);

        // Use the same DN for issuer and subject.
        X500Name dnName = new X500Name("CN=Dummy Server");

        // Use current timestamp as serial number.
        BigInteger certSerialNumber = BigInteger.valueOf(now);

        // Certificate valid for 10 years.
        Date endDate = new Date(now + 10L * 365 * 24 * 60 * 60 * 1000);

        // Build the content signer.
        ContentSigner contentSigner =
                new JcaContentSignerBuilder("SHA256WithRSAEncryption").build(keyPair.getPrivate());

        // Build the certificate.
        JcaX509v3CertificateBuilder certBuilder =
                new JcaX509v3CertificateBuilder(
                        dnName, certSerialNumber, startDate, endDate, dnName, keyPair.getPublic());

        X509CertificateHolder certHolder = certBuilder.build(contentSigner);
        X509Certificate cert = new JcaX509CertificateConverter().getCertificate(certHolder);
        // Verify the certificate.
        cert.verify(keyPair.getPublic());
        return cert;
    }

    // Create a temporary keystore file containing the self-signed certificate.
    public static String createTempKeyStore(KeyPair keyPair, X509Certificate cert)
            throws Exception {
        java.security.KeyStore keyStore = java.security.KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        keyStore.setKeyEntry(
                "server", keyPair.getPrivate(), STORE_PASSWORD, new X509Certificate[] {cert});
        File tempKeyStore = File.createTempFile("test-keystore", ".jks");
        tempKeyStore.deleteOnExit();
        try (FileOutputStream fos = new FileOutputStream(tempKeyStore)) {
            keyStore.store(fos, STORE_PASSWORD);
        }
        return tempKeyStore.getAbsolutePath();
    }

    // Create a temporary truststore file containing the certificate.
    public static String createTempTrustStore(X509Certificate cert) throws Exception {
        java.security.KeyStore trustStore = java.security.KeyStore.getInstance("JKS");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("server", cert);
        File tempTrustStore = File.createTempFile("test-truststore", ".jks");
        tempTrustStore.deleteOnExit();
        try (FileOutputStream fos = new FileOutputStream(tempTrustStore)) {
            trustStore.store(fos, STORE_PASSWORD);
        }
        return tempTrustStore.getAbsolutePath();
    }

    public static DummySSLStores generateDummySSLStores() throws Exception {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048, new SecureRandom());
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        X509Certificate cert = generateSelfSignedCertificate(keyPair);
        String keystorePath = createTempKeyStore(keyPair, cert);
        String truststorePath = createTempTrustStore(cert);
        return new DummySSLStores(keystorePath, truststorePath);
    }

    public static class DummySSLStores {
        public final String keystorePath;
        public final String truststorePath;

        public DummySSLStores(String keystorePath, String truststorePath) {
            this.keystorePath = keystorePath;
            this.truststorePath = truststorePath;
        }
    }
}
