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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class S3Utils implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(S3Utils.class);
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final String REGION = "cn-north-1";
    private static final String DEFAULT_ENDPOINT = "http://localhost:9000";
    private static final String BUCKET = "ws-package";

    private static AmazonS3 s3Client;

    public static synchronized void initialize(String endpoint) {
        if (s3Client != null) {
            s3Client.shutdown();
        }
        BasicAWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);
        s3Client =
                AmazonS3ClientBuilder.standard()
                        .withCredentials(new AWSStaticCredentialsProvider(credentials))
                        .enablePathStyleAccess()
                        .withEndpointConfiguration(
                                new AwsClientBuilder.EndpointConfiguration(endpoint, REGION))
                        .build();

        if (!s3Client.doesBucketExistV2(BUCKET)) {
            s3Client.createBucket(BUCKET);
        }
    }

    private static synchronized AmazonS3 getS3Client() {
        if (s3Client == null) {
            initialize(DEFAULT_ENDPOINT);
        }
        return s3Client;
    }

    public static void uploadTestFiles(
            String filePath, String targetFilePath, boolean isFindFromResource) {
        File resourcesFile = null;
        if (isFindFromResource) {
            resourcesFile = ContainerUtil.getResourcesFile(filePath);
        } else {
            resourcesFile = new File(filePath);
        }
        getS3Client().putObject(BUCKET, targetFilePath, resourcesFile);
    }

    public static void createDir(String dir) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
        PutObjectRequest putObjectRequest =
                new PutObjectRequest(BUCKET, dir, emptyContent, metadata);
        getS3Client().putObject(putObjectRequest);
    }

    public static void uploadContent(String targetFilePath, String content) {
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentBytes.length);
        getS3Client()
                .putObject(
                        new PutObjectRequest(
                                BUCKET,
                                targetFilePath,
                                new ByteArrayInputStream(contentBytes),
                                metadata));
    }

    public static String readContent(String targetFilePath) throws IOException {
        try (S3Object object = getS3Client().getObject(BUCKET, targetFilePath);
                InputStream inputStream = object.getObjectContent();
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    public static boolean objectExists(String targetFilePath) {
        return getS3Client().doesObjectExist(BUCKET, targetFilePath);
    }

    public static void deletePrefix(String prefix) {
        getS3Client()
                .listObjectsV2(BUCKET, prefix)
                .getObjectSummaries()
                .forEach(summary -> getS3Client().deleteObject(BUCKET, summary.getKey()));
    }

    @Override
    public void close() throws Exception {
        if (s3Client != null) {
            s3Client.shutdown();
        }
    }
}
