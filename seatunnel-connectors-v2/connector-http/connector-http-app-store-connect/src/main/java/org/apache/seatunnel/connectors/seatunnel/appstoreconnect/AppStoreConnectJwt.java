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
package org.apache.seatunnel.connectors.seatunnel.appstoreconnect;

import java.nio.charset.StandardCharsets;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;

/**
 * Signs Apple team API JWTs with a P-256 private key.
 *
 * <p>JCA's P1363 signature format is the fixed-width JOSE r||s representation. It avoids ASN.1
 * signature conversion and third-party JWT dependencies on Java 11.
 */
final class AppStoreConnectJwt {
    private static final Base64.Encoder BASE64 = Base64.getUrlEncoder().withoutPadding();

    // A 15-minute token is within Apple's standard 20-minute limit.
    private static final int TOKEN_SECONDS = 900;

    private final String issuer;
    private final String header;
    private final PrivateKey key;

    // Refresh before expiry and reject cached tokens if the wall clock moves backward.
    private Instant issued;

    private String token;

    AppStoreConnectJwt(AppStoreConnectConfig config) {
        issuer = config.issuer;
        header = encode("{\"alg\":\"ES256\",\"kid\":\"" + config.keyId + "\",\"typ\":\"JWT\"}");
        key = loadKey(config.secretKey);
    }

    /**
     * Loads only PKCS#8 P-256 signing keys. Do not attach parser errors which may expose key data.
     */
    static PrivateKey loadKey(String value) {
        if (value == null || value.length() > 8192) {
            throw AppStoreConnectConfig.invalid(AppStoreConnectSourceOptions.SECRET_KEY);
        }
        byte[] decoded = null;
        try {
            String body = value.trim();
            if (body.startsWith("-----BEGIN PRIVATE KEY-----")
                    && body.endsWith("-----END PRIVATE KEY-----")) {
                body = body.substring(27, body.length() - 25);
            }
            decoded = Base64.getDecoder().decode(body.replaceAll("\\s+", ""));
            PrivateKey parsed =
                    KeyFactory.getInstance("EC").generatePrivate(new PKCS8EncodedKeySpec(decoded));
            if (parsed instanceof ECPrivateKey) {
                ECPrivateKey ec = (ECPrivateKey) parsed;
                AlgorithmParameters params = AlgorithmParameters.getInstance("EC");
                params.init(new ECGenParameterSpec("secp256r1"));
                ECParameterSpec expected = params.getParameterSpec(ECParameterSpec.class);
                ECParameterSpec actual = ec.getParams();
                if (actual != null
                        && actual.getCurve().equals(expected.getCurve())
                        && actual.getGenerator().equals(expected.getGenerator())
                        && actual.getOrder().equals(expected.getOrder())
                        && actual.getCofactor() == expected.getCofactor()
                        && ec.getS().signum() > 0
                        && ec.getS().compareTo(expected.getOrder()) < 0) {
                    return ec;
                }
            }
        } catch (GeneralSecurityException | IllegalArgumentException ignored) {
            // Use a fixed error for malformed credentials.
        } finally {
            if (decoded != null) {
                Arrays.fill(decoded, (byte) 0);
            }
        }
        throw AppStoreConnectConfig.invalid(AppStoreConnectSourceOptions.SECRET_KEY);
    }

    String bearer(AppStoreConnectResource resource, Instant now) {
        if (now.getEpochSecond() <= 0) {
            throw resource.failure("system time cannot be used to sign an API token");
        }
        if (token == null || now.isBefore(issued) || !now.isBefore(issued.plusSeconds(870))) {
            long iat = now.getEpochSecond();
            String payload =
                    "{\"iss\":\""
                            + issuer
                            + "\",\"aud\":\"appstoreconnect-v1\",\"iat\":"
                            + iat
                            + ",\"exp\":"
                            + (iat + TOKEN_SECONDS)
                            + "}";
            String unsigned = header + "." + encode(payload);
            try {
                Signature signature = Signature.getInstance("SHA256withECDSAinP1363Format");
                signature.initSign(key);
                signature.update(unsigned.getBytes(StandardCharsets.US_ASCII));
                byte[] signed = signature.sign();
                if (signed.length != 64) {
                    throw resource.failure("JCA did not produce a 64-byte ES256 signature");
                }
                issued = now;
                token = unsigned + "." + BASE64.encodeToString(signed);
            } catch (GeneralSecurityException ex) {
                throw resource.failure("could not sign the team API token");
            }
        }
        return "Bearer " + token;
    }

    void invalidate() {
        token = null;
    }

    private static String encode(String value) {
        return BASE64.encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
