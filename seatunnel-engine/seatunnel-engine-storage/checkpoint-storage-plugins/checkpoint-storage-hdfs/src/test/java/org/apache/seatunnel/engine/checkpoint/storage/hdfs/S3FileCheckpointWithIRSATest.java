/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;

import java.util.HashMap;
import java.util.Map;

/**
 * Test S3 checkpoint storage using Kubernetes IRSA (IAM Roles for Service Accounts).
 *
 * <p>This test uses DefaultAWSCredentialsProviderChain which supports: 1. Environment variables
 * (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) 2. Java system properties 3. Web Identity Token
 * (Kubernetes IRSA) - reads from AWS_WEB_IDENTITY_TOKEN_FILE 4. EC2 instance profile credentials 5.
 * ECS container credentials
 *
 * <p>To run this test in Kubernetes with IRSA: 1. Create a Kubernetes service account with IRSA
 * annotation: kubectl create serviceaccount seatunnel-sa kubectl annotate serviceaccount
 * seatunnel-sa \ eks.amazonaws.com/role-arn=arn:aws:iam::YOUR_ACCOUNT:role/YOUR_ROLE
 *
 * <p>2. Deploy the test pod with this service account
 *
 * <p>3. The AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN environment variables will be
 * automatically set by EKS
 *
 * <p>Alternatively, for local testing with environment variables: export AWS_ACCESS_KEY_ID=your-key
 * export AWS_SECRET_ACCESS_KEY=your-secret export AWS_REGION=your-region
 */
@Disabled(
        "IRSA requires Kubernetes environment with proper IAM role setup. "
                + "Enable this test when running in EKS with IRSA configured.")
public class S3FileCheckpointWithIRSATest extends AbstractFileCheckPointTest {

    @BeforeAll
    public static void setup() throws CheckpointStorageException {
        Map<String, String> config = new HashMap<>();
        config.put("storage.type", "s3");
        config.put("disable.cache", "false");

        // Set your S3 bucket - replace with your actual bucket name
        config.put("s3.bucket", "s3a://your-test-bucket");

        // Optional: set endpoint for non-AWS S3-compatible storage
        // config.put("fs.s3a.endpoint", "https://s3.us-west-2.amazonaws.com");

        // Use DefaultAWSCredentialsProviderChain for automatic credential detection
        // This will work with IRSA, environment variables, EC2 instance profiles, etc.
        config.put(
                "fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        STORAGE = new HdfsStorage(config);
        initStorageData();
    }
}
