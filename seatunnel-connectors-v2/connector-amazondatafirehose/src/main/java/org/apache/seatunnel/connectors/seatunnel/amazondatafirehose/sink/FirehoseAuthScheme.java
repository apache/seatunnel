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

package org.apache.seatunnel.connectors.seatunnel.amazondatafirehose.sink;

import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.scheme.AuthScheme;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.AwsSessionCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.IdentityProviders;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * Signs only the Firehose POST-root SigV4 contract without logging canonical credentials. The
 * pinned SDK default signer logs the session token at DEBUG, so the replacement is client-local.
 */
final class FirehoseAuthScheme
        implements AuthScheme<AwsCredentialsIdentity>, HttpSigner<AwsCredentialsIdentity> {
    private static final String ALGORITHM = "AWS4-HMAC-SHA256";
    private static final DateTimeFormatter TIMESTAMP =
            DateTimeFormatter.ofPattern("uuuuMMdd'T'HHmmss'Z'", Locale.ROOT)
                    .withZone(ZoneOffset.UTC);
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    @Override
    public String schemeId() {
        return "aws.auth#sigv4";
    }

    @Override
    public IdentityProvider<AwsCredentialsIdentity> identityProvider(IdentityProviders providers) {
        return providers.identityProvider(AwsCredentialsIdentity.class);
    }

    @Override
    public HttpSigner<AwsCredentialsIdentity> signer() {
        return this;
    }

    /*
     * Hash the repeatable marshalled payload and sign normalized host/content-type/x-amz headers.
     * The constrained root path avoids generic URI canonicalization; no global signer/logger is changed.
     */
    @Override
    public SignedRequest sign(SignRequest<? extends AwsCredentialsIdentity> input) {
        SdkHttpRequest original = input.request();
        if (original.method() != SdkHttpMethod.POST
                || !"/".equals(original.encodedPath())
                || !original.rawQueryParameters().isEmpty()) {
            throw new IllegalArgumentException("Unexpected Firehose signing target");
        }
        AwsCredentialsIdentity identity = input.identity();
        if (!identity.accessKeyId().matches("[A-Za-z0-9]{1,128}")) {
            throw new IllegalArgumentException("Invalid AWS access key");
        }
        Clock clock = input.requireProperty(SIGNING_CLOCK, Clock.systemUTC());
        String timestamp = TIMESTAMP.format(clock.instant());
        String region = input.requireProperty(AwsV4HttpSigner.REGION_NAME);
        String date = timestamp.substring(0, 8);
        ContentStreamProvider payload =
                input.payload()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Missing Firehose request payload"));
        String host = original.getUri().getRawAuthority();
        SdkHttpRequest.Builder builder =
                original.toBuilder().putHeader("Host", host).putHeader("X-Amz-Date", timestamp);
        if (identity instanceof AwsSessionCredentialsIdentity) {
            String token = ((AwsSessionCredentialsIdentity) identity).sessionToken();
            if (token.isEmpty()
                    || token.length() > 8192
                    || token.chars().anyMatch(c -> c < 0x21 || c > 0x7e)) {
                throw new IllegalArgumentException("Invalid AWS session token");
            }
            builder.putHeader("X-Amz-Security-Token", token);
        }
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            int bytes = 0;
            try (InputStream stream = payload.newStream()) {
                byte[] chunk = new byte[8192];
                int n;
                while ((n = stream.read(chunk)) != -1) {
                    bytes += n;
                    if (bytes > AmazonDataFirehoseSinkWriter.REQUEST_MAX_BYTES) {
                        throw new IllegalArgumentException("Firehose request exceeds 4 MiB");
                    }
                    sha.update(chunk, 0, n);
                }
            }
            String payloadHash = hex(sha.digest());
            SdkHttpRequest request =
                    builder.putHeader("Content-Length", Integer.toString(bytes)).build();
            Map<String, String> signed = new TreeMap<>();
            request.forEachHeader(
                    (name, values) -> {
                        String key = name.toLowerCase(Locale.ROOT);
                        if ("host".equals(key)
                                || "content-type".equals(key)
                                || key.startsWith("x-amz-")) {
                            List<String> normalized = new ArrayList<>(values.size());
                            values.forEach(
                                    value ->
                                            normalized.add(
                                                    value.trim().replaceAll("[ \\t]+", " ")));
                            signed.put(key, String.join(",", normalized));
                        }
                    });
            String signedNames = String.join(";", signed.keySet());
            StringBuilder canonical = new StringBuilder("POST\n/\n\n");
            signed.forEach(
                    (key, value) -> canonical.append(key).append(':').append(value).append('\n'));
            canonical.append('\n').append(signedNames).append('\n').append(payloadHash);
            String scope = date + "/" + region + "/firehose/aws4_request";
            String toSign =
                    ALGORITHM
                            + "\n"
                            + timestamp
                            + "\n"
                            + scope
                            + "\n"
                            + hex(
                                    sha.digest(
                                            canonical.toString().getBytes(StandardCharsets.UTF_8)));
            Mac mac = Mac.getInstance("HmacSHA256");
            byte[] key =
                    hmac(
                            mac,
                            ("AWS4" + identity.secretAccessKey()).getBytes(StandardCharsets.UTF_8),
                            date);
            key = hmac(mac, key, region);
            key = hmac(mac, key, "firehose");
            key = hmac(mac, key, "aws4_request");
            String signature = hex(hmac(mac, key, toSign));
            builder.putHeader(
                    "Authorization",
                    ALGORITHM
                            + " Credential="
                            + identity.accessKeyId()
                            + "/"
                            + scope
                            + ", SignedHeaders="
                            + signedNames
                            + ", Signature="
                            + signature);
            return SignedRequest.builder().request(builder.build()).payload(payload).build();
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("Could not sign the Firehose request");
        }
    }

    /*
     * Each scope step reinitializes the request-local MAC with its derived key; no signing state is shared.
     */
    private static byte[] hmac(Mac mac, byte[] key, String data) throws GeneralSecurityException {
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    }

    private static String hex(byte[] data) {
        char[] result = new char[data.length * 2];
        for (int i = 0; i < data.length; i++) {
            int value = data[i] & 0xff;
            result[i * 2] = HEX[value >>> 4];
            result[i * 2 + 1] = HEX[value & 15];
        }
        return new String(result);
    }

    @Override
    public CompletableFuture<AsyncSignedRequest> signAsync(
            AsyncSignRequest<? extends AwsCredentialsIdentity> request) {
        CompletableFuture<AsyncSignedRequest> result = new CompletableFuture<>();
        result.completeExceptionally(
                new UnsupportedOperationException("Firehose transport is synchronous"));
        return result;
    }
}
