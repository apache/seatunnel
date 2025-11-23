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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;

public class S3Utils implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(S3Utils.class);
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final String REGION = "cn-north-1";
    private static final String ENDPOINT = "http://localhost:9000";
    private static final String BUCKET = "ws-package";

    private static final AmazonS3 S3_CLIENT;

    static {
        BasicAWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);
        S3_CLIENT =
                AmazonS3ClientBuilder.standard()
                        .withCredentials(new AWSStaticCredentialsProvider(credentials))
                        .enablePathStyleAccess()
                        .withEndpointConfiguration(
                                new AwsClientBuilder.EndpointConfiguration(ENDPOINT, REGION))
                        .build();

        if (!S3_CLIENT.doesBucketExistV2(BUCKET)) {
            S3_CLIENT.createBucket(BUCKET);
        }
    }

    public static void uploadTestFiles(
            String filePath, String targetFilePath, boolean isFindFromResource) {
        File resourcesFile = null;
        if (isFindFromResource) {
            resourcesFile = ContainerUtil.getResourcesFile(filePath);
        } else {
            resourcesFile = new File(filePath);
        }
        S3_CLIENT.putObject(BUCKET, targetFilePath, resourcesFile);
    }

    public static void createDir(String dir) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
        PutObjectRequest putObjectRequest =
                new PutObjectRequest(BUCKET, dir, emptyContent, metadata);
        S3_CLIENT.putObject(putObjectRequest);
    }

    @Override
    public void close() throws Exception {
        if (S3_CLIENT != null) {
            S3_CLIENT.shutdown();
        }
    }
}
