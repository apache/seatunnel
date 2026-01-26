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

package org.apache.seatunnel.connectors.seatunnel.iceberg.aws;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsCredentials;

import java.util.HashMap;
import java.util.Map;

class StaticAwsCredentialsProviderTest {

    @Test
    void testCreateAndResolveCredentialsSuccess() {
        Map<String, String> properties = new HashMap<>();
        properties.put("access-key-id", " test-access-key ");
        properties.put("secret-access-key", "test-secret-key");

        StaticAwsCredentialsProvider provider = StaticAwsCredentialsProvider.create(properties);
        AwsCredentials credentials = provider.resolveCredentials();

        // Verify values are correctly mapped and trimmed
        Assertions.assertEquals("test-access-key", credentials.accessKeyId());
        Assertions.assertEquals("test-secret-key", credentials.secretAccessKey());
    }

    @Test
    void testCreateMissingAccessKey() {
        Map<String, String> properties = new HashMap<>();
        properties.put("secret-access-key", "test-secret-key");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> StaticAwsCredentialsProvider.create(properties));

        Assertions.assertTrue(exception.getMessage().contains("Missing credentials"));
    }

    @Test
    void testCreateMissingSecretKey() {
        Map<String, String> properties = new HashMap<>();
        properties.put("access-key-id", "test-access-key");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> StaticAwsCredentialsProvider.create(properties));

        Assertions.assertTrue(exception.getMessage().contains("Missing credentials"));
    }

    @Test
    void testCreateWithEmptyProperties() {
        Map<String, String> properties = new HashMap<>();

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> StaticAwsCredentialsProvider.create(properties));
    }
}
