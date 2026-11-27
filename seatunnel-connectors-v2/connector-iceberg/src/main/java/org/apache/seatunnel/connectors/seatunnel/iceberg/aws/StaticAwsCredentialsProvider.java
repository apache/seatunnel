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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.util.Map;

/**
 * Supplies credentials to Iceberg AWS clients, including Glue and the default S3 fallback. Iceberg
 * creates the provider reflectively after removing the credentials-provider property prefix.
 */
public class StaticAwsCredentialsProvider implements AwsCredentialsProvider {

    private final StaticCredentialsProvider delegate;

    private StaticAwsCredentialsProvider(Map<String, String> properties) {
        // Properties passed here have the prefix "client.credentials-provider." removed
        String accessKey = properties.get("access-key-id");
        String secretKey = properties.get("secret-access-key");

        if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
            throw new IllegalArgumentException(
                    "Missing credentials. Use 'client.credentials-provider.access-key-id' "
                            + "and 'client.credentials-provider.secret-access-key' in your catalog configuration.");
        }

        this.delegate =
                StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey.trim(), secretKey.trim()));
    }

    /**
     * Creates the provider through Iceberg's reflective credentials-provider extension point.
     *
     * @param properties provider options after Iceberg removes the credentials-provider prefix
     * @return a provider with nonblank access and secret keys
     */
    public static StaticAwsCredentialsProvider create(Map<String, String> properties) {
        return new StaticAwsCredentialsProvider(properties);
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return delegate.resolveCredentials();
    }
}
