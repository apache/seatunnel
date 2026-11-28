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

package org.apache.seatunnel.connectors.seatunnel.awscloudtrail;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 * Signs the exact bytes of the fixed AWS CloudTrail LookupEvents request with SigV4. Temporary
 * credential headers and the RPC target are included in the signed header set.
 */
final class AwsCloudTrailSigner {
    static final String TARGET =
            "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents";
    static final String CONTENT = "application/x-amz-json-1.1";
    private static final String ALGORITHM = "AWS4-HMAC-SHA256";
    private static final String HEX = "0123456789abcdef";
    private static final DateTimeFormatter DATE =
            DateTimeFormatter.ofPattern("uuuuMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    private final AwsCloudTrailConfig config;

    AwsCloudTrailSigner(AwsCloudTrailConfig config) {
        this.config = config;
    }

    /**
     * Returns normalized signed headers and Authorization for one timed request. The request
     * payload is passed unchanged to the HTTPS transport.
     */
    Map<String, String> sign(byte[] payload, Instant now) {
        String stamp = DATE.format(now);
        String date = stamp.substring(0, 8);
        String hash = hex(AwsCloudTrailJson.sha256(payload));
        Map<String, String> headers = new TreeMap<>();
        headers.put("content-type", CONTENT);
        headers.put("host", config.host);
        headers.put("x-amz-content-sha256", hash);
        headers.put("x-amz-date", stamp);
        if (!config.securityToken.isEmpty()) {
            headers.put("x-amz-security-token", config.securityToken);
        }
        headers.put("x-amz-target", TARGET);
        String signed = String.join(";", headers.keySet());
        StringBuilder canonical = new StringBuilder("POST\n/\n\n");
        headers.forEach(
                (name, value) -> canonical.append(name).append(':').append(value).append('\n'));
        canonical.append('\n').append(signed).append('\n').append(hash);
        String scope = date + "/" + config.region + "/cloudtrail/aws4_request";
        String toSign =
                ALGORITHM
                        + "\n"
                        + stamp
                        + "\n"
                        + scope
                        + "\n"
                        + hex(
                                AwsCloudTrailJson.sha256(
                                        canonical.toString().getBytes(StandardCharsets.UTF_8)));
        byte[] key = ("AWS4" + config.secret).getBytes(StandardCharsets.US_ASCII);
        try {
            key = derive(key, date);
            key = derive(key, config.region);
            key = derive(key, "cloudtrail");
            key = derive(key, "aws4_request");
            String signature = hex(hmac(key, toSign));
            headers.put(
                    "authorization",
                    ALGORITHM
                            + " Credential="
                            + config.access
                            + "/"
                            + scope
                            + ", SignedHeaders="
                            + signed
                            + ", Signature="
                            + signature);
            return headers;
        } finally {
            Arrays.fill(key, (byte) 0);
        }
    }

    private static byte[] derive(byte[] previous, String message) {
        try {
            return hmac(previous, message);
        } finally {
            Arrays.fill(previous, (byte) 0);
        }
    }

    private static byte[] hmac(byte[] key, String message) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(message.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AWS CloudTrail source: HMAC-SHA256 unavailable", e);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte part : bytes) {
            value.append(HEX.charAt((part & 255) >>> 4)).append(HEX.charAt(part & 15));
        }
        return value.toString();
    }
}
