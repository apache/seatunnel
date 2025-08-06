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

package org.apache.seatunnel.e2e.connector.file.s3;

import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;

public class S3Utils {
    private static Logger logger = LoggerFactory.getLogger(S3Utils.class);
    private static final String ACCESS_KEY = "XXXXXX";
    private static final String SECRET_KEY = "AWS_XXXX";
    private static final String REGION = "cn-north-1";
    private static final String ENDPOINT =
            "s3.cn-north-1.amazonaws.com.cn"; // For example, "https://s3.amazonaws.com"
    private String bucket = "ws-package";

    private final S3Client s3Client;

    public S3Utils() {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY);

        this.s3Client =
                S3Client.builder()
                        .credentialsProvider(StaticCredentialsProvider.create(credentials))
                        .endpointOverride(java.net.URI.create(ENDPOINT))
                        .region(Region.of(REGION))
                        .serviceConfiguration(
                                S3Configuration.builder().pathStyleAccessEnabled(true).build())
                        .build();
    }

    public void uploadTestFiles(
            String filePath, String targetFilePath, boolean isFindFromResource) {
        File resourcesFile = null;
        if (isFindFromResource) {
            resourcesFile = ContainerUtil.getResourcesFile(filePath);
        } else {
            resourcesFile = new File(filePath);
        }
        PutObjectRequest putObjectRequest =
                PutObjectRequest.builder().bucket(bucket).key(targetFilePath).build();
        s3Client.putObject(putObjectRequest, RequestBody.fromFile(resourcesFile));
    }

    public void createDir(String dir) {
        if (!dir.endsWith("/")) {
            dir = dir + "/";
        }

        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
        PutObjectRequest putObjectRequest =
                PutObjectRequest.builder().bucket(bucket).key(dir).build();
        s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(emptyContent, 0));
    }

    public void close() {
        if (s3Client != null) {
            s3Client.close();
        }
    }
}
