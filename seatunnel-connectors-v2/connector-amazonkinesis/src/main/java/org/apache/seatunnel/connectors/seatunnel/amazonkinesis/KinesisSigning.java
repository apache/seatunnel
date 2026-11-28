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

package org.apache.seatunnel.connectors.seatunnel.amazonkinesis;

import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.scheme.AwsV4AuthScheme;
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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
import java.util.stream.Collectors;

/**
 * SigV4 for Kinesis's fixed POST root. The pinned SDK signer logs the canonical request (including
 * the session token) at DEBUG. This client-local scheme uses the SDK's identity and region
 * resolution.
 */
final class KinesisSigning
        implements AuthScheme<AwsCredentialsIdentity>, HttpSigner<AwsCredentialsIdentity> {
    private static final DateTimeFormatter DATE =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'", Locale.ROOT)
                    .withZone(ZoneOffset.UTC);
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    @Override
    public String schemeId() {
        return AwsV4AuthScheme.SCHEME_ID;
    }

    @Override
    public IdentityProvider<AwsCredentialsIdentity> identityProvider(IdentityProviders providers) {
        return AwsV4AuthScheme.create().identityProvider(providers);
    }

    @Override
    public HttpSigner<AwsCredentialsIdentity> signer() {
        return this;
    }

    /*
     * Sign the constrained POST-root contract with normalized host/content-type/x-amz headers.
     * The client-local signer never logs a canonical request, whose session-token header is sensitive.
     */
    @Override
    public SignedRequest sign(SignRequest<? extends AwsCredentialsIdentity> input) {
        SdkHttpRequest unsigned = input.request();
        if (unsigned.method() != SdkHttpMethod.POST
                || !"/".equals(unsigned.encodedPath())
                || !unsigned.rawQueryParameters().isEmpty()) {
            throw new IllegalStateException("Unsupported Kinesis signing target");
        }
        String region = input.requireProperty(AwsV4HttpSigner.REGION_NAME);
        Clock clock = input.property(HttpSigner.SIGNING_CLOCK);
        String time = DATE.format((clock == null ? Clock.systemUTC() : clock).instant());
        String day = time.substring(0, 8);
        String scope = day + "/" + region + "/kinesis/aws4_request";
        AwsCredentialsIdentity credentials = input.identity();

        try {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            if (input.payload().isPresent()) {
                try (InputStream data = input.payload().get().newStream()) {
                    byte[] part = new byte[8192];
                    int count;
                    int total = 0;
                    while ((count = data.read(part)) != -1) {
                        total += count;
                        if (total > KinesisTransport.MAX_WIRE_BYTES) {
                            throw new IllegalStateException(
                                    "Kinesis signed body exceeds the transport limit");
                        }
                        hash.update(part, 0, count);
                    }
                }
            }
            String contentHash = hex(hash.digest());
            SdkHttpRequest.Builder request =
                    unsigned.toBuilder()
                            .putHeader("Host", unsigned.getUri().getRawAuthority())
                            .putHeader("X-Amz-Date", time)
                            .putHeader("X-Amz-Content-Sha256", contentHash);
            if (credentials instanceof AwsSessionCredentialsIdentity) {
                request.putHeader(
                        "X-Amz-Security-Token",
                        ((AwsSessionCredentialsIdentity) credentials).sessionToken());
            }
            Map<String, List<String>> signed = new TreeMap<>();
            for (Map.Entry<String, List<String>> h : request.build().headers().entrySet()) {
                String name = h.getKey().toLowerCase(Locale.ROOT);
                if (name.equals("host")
                        || name.equals("content-type")
                        || name.startsWith("x-amz-")) {
                    signed.computeIfAbsent(name, unused -> new ArrayList<>()).addAll(h.getValue());
                }
            }
            String names = String.join(";", signed.keySet());
            String headers =
                    signed.entrySet().stream()
                            .map(
                                    h ->
                                            h.getKey()
                                                    + ":"
                                                    + h.getValue().stream()
                                                            .map(
                                                                    v ->
                                                                            v.trim()
                                                                                    .replaceAll(
                                                                                            "[ \\t]+",
                                                                                            " "))
                                                            .collect(Collectors.joining(","))
                                                    + "\n")
                            .collect(Collectors.joining());
            String canonical = "POST\n/\n\n" + headers + "\n" + names + "\n" + contentHash;
            String toSign =
                    "AWS4-HMAC-SHA256\n"
                            + time
                            + "\n"
                            + scope
                            + "\n"
                            + hex(hash.digest(canonical.getBytes(StandardCharsets.UTF_8)));
            byte[] key =
                    hmac(
                            ("AWS4" + credentials.secretAccessKey())
                                    .getBytes(StandardCharsets.UTF_8),
                            day);
            key = hmac(hmac(hmac(key, region), "kinesis"), "aws4_request");
            request.putHeader(
                    "Authorization",
                    "AWS4-HMAC-SHA256 Credential="
                            + credentials.accessKeyId()
                            + "/"
                            + scope
                            + ", SignedHeaders="
                            + names
                            + ", Signature="
                            + hex(hmac(key, toSign)));
            SignedRequest.Builder result = SignedRequest.builder().request(request.build());
            input.payload().ifPresent(result::payload);
            return result.build();
        } catch (Exception ex) {
            // Even SDK or cryptographic exception text can contain request contents.
            throw new IllegalStateException("Failed to sign Kinesis request");
        }
    }

    @Override
    public CompletableFuture<AsyncSignedRequest> signAsync(
            AsyncSignRequest<? extends AwsCredentialsIdentity> request) {
        CompletableFuture<AsyncSignedRequest> result = new CompletableFuture<>();
        result.completeExceptionally(
                new UnsupportedOperationException("Kinesis uses synchronous signing"));
        return result;
    }

    private static byte[] hmac(byte[] key, String value) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String hex(byte[] bytes) {
        char[] value = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xff;
            value[2 * i] = HEX[b >>> 4];
            value[2 * i + 1] = HEX[b & 0x0f];
        }
        return new String(value);
    }
}
