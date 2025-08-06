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

package org.apache.seatunnel.e2e.connector.databend;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

@Slf4j
public class DatabendTestUtils {
    /**
     * using AWS SDK create MinIO bucket
     *
     * @param bucketName bucket
     * @return success or not
     */
    public static boolean createMinIOBucketWithAWSSDK(String bucketName) {
        try {
            log.info("using AWS SDK to create MinIO bucket: {}", bucketName);

            S3Client s3Client =
                    S3Client.builder()
                            .endpointOverride(java.net.URI.create("http://localhost:9000"))
                            .region(Region.US_EAST_1)
                            .credentialsProvider(
                                    StaticCredentialsProvider.create(
                                            AwsBasicCredentials.create("minioadmin", "minioadmin")))
                            .serviceConfiguration(
                                    S3Configuration.builder().pathStyleAccessEnabled(true).build())
                            .build();

            try {
                HeadBucketRequest headBucketRequest =
                        HeadBucketRequest.builder().bucket(bucketName).build();
                s3Client.headBucket(headBucketRequest);
                log.info("bucket {} exist，no need to create", bucketName);
                s3Client.close();
                return true;
            } catch (NoSuchBucketException e) {
                log.info("bucket {} does not exist, creating...", bucketName);
            }

            CreateBucketRequest createBucketRequest =
                    CreateBucketRequest.builder().bucket(bucketName).build();
            s3Client.createBucket(createBucketRequest);

            log.info("create MinIO bucket success: {}", bucketName);
            s3Client.close();
            return true;
        } catch (Exception e) {
            log.error("using AWS SDK to create MinIO failed", e);
            return false;
        }
    }
}
