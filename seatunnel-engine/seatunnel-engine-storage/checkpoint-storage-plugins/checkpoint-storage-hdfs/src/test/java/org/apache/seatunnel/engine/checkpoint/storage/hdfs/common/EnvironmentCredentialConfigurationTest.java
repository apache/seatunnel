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

package org.apache.seatunnel.engine.checkpoint.storage.hdfs.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils;
import org.apache.hadoop.fs.cosn.CosNUtils;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.S3AUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.qcloud.cos.auth.COSCredentials;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;

/**
 * Tests mutate process-wide environment variables through {@code SystemLambda}. {@code SAME_THREAD}
 * keeps the methods in this class sequential, and the shared resource lock keeps other environment-
 * mutating test classes from running concurrently.
 */
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("system-environment")
public class EnvironmentCredentialConfigurationTest {

    private static final String S3_PROVIDER_KEY = "fs.s3a.aws.credentials.provider";
    private static final String S3_ENV_PROVIDER =
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider";

    private static final String OSS_PROVIDER_KEY = "fs.oss.credentials.provider";
    private static final String OSS_ENV_PROVIDER =
            "com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider";

    private static final String COS_PROVIDER_KEY = "fs.cosn.credentials.provider";
    private static final String COS_ENV_PROVIDER =
            "org.apache.hadoop.fs.cosn.auth.EnvironmentVariableCredentialsProvider";

    private static final String S3_ACCESS_KEY = "test-s3-access-key";
    private static final String S3_SECRET_KEY = "test-s3-secret-key";
    private static final String S3_SESSION_TOKEN = "test-s3-session-token";

    private static final String OSS_ACCESS_KEY = "test-oss-access-key";
    private static final String OSS_SECRET_KEY = "test-oss-secret-key";
    private static final String OSS_SESSION_TOKEN = "test-oss-session-token";

    private static final String COS_SECRET_ID = "test-cos-secret-id";
    private static final String COS_SECRET_KEY = "test-cos-secret-key";

    @Test
    public void testS3EnvironmentCredentialProviderIsPassedThrough() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("s3.bucket", "s3a://seatunnel-checkpoint");
        config.put(S3_PROVIDER_KEY, S3_ENV_PROVIDER);

        Configuration hadoopConf =
                FileConfiguration.valueOf("S3").getConfiguration().buildConfiguration(config);

        Assertions.assertEquals(S3_ENV_PROVIDER, hadoopConf.get(S3_PROVIDER_KEY));
        Assertions.assertNull(hadoopConf.get("fs.s3a.access.key"));
        Assertions.assertNull(hadoopConf.get("fs.s3a.secret.key"));
    }

    @Test
    public void testOssEnvironmentCredentialProviderIsPassedThrough() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("oss.bucket", "oss://seatunnel-checkpoint");
        config.put("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        config.put(OSS_PROVIDER_KEY, OSS_ENV_PROVIDER);

        Configuration hadoopConf =
                FileConfiguration.valueOf("OSS").getConfiguration().buildConfiguration(config);

        Assertions.assertEquals(OSS_ENV_PROVIDER, hadoopConf.get(OSS_PROVIDER_KEY));
        Assertions.assertNull(hadoopConf.get("fs.oss.accessKeyId"));
        Assertions.assertNull(hadoopConf.get("fs.oss.accessKeySecret"));
    }

    @Test
    public void testCosEnvironmentCredentialProviderIsPassedThrough() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("cos.bucket", "cosn://seatunnel-checkpoint");
        config.put("fs.cosn.bucket.region", "ap-shanghai");
        config.put(COS_PROVIDER_KEY, COS_ENV_PROVIDER);

        Configuration hadoopConf =
                FileConfiguration.valueOf("COS").getConfiguration().buildConfiguration(config);

        Assertions.assertEquals(COS_ENV_PROVIDER, hadoopConf.get(COS_PROVIDER_KEY));
        Assertions.assertNull(hadoopConf.get("fs.cosn.userinfo.secretId"));
        Assertions.assertNull(hadoopConf.get("fs.cosn.userinfo.secretKey"));
    }

    /**
     * Guards that the provider classes are present on the test classpath; the tests below verify
     * that they actually read the environment variables.
     */
    @Test
    public void testEnvironmentCredentialProviderClassesAreOnTestClasspath() {
        Assertions.assertDoesNotThrow(() -> Class.forName(S3_ENV_PROVIDER));
        Assertions.assertDoesNotThrow(() -> Class.forName(OSS_ENV_PROVIDER));
        Assertions.assertDoesNotThrow(() -> Class.forName(COS_ENV_PROVIDER));
    }

    @Test
    public void testS3EnvironmentProviderReadsConfiguredValues() throws Exception {
        withEnvironmentVariable("AWS_ACCESS_KEY_ID", S3_ACCESS_KEY)
                .and("AWS_SECRET_ACCESS_KEY", S3_SECRET_KEY)
                .and("AWS_SESSION_TOKEN", S3_SESSION_TOKEN)
                .execute(
                        () -> {
                            Map<String, String> config = new HashMap<>();
                            config.put("s3.bucket", "s3a://seatunnel-checkpoint");
                            config.put(S3_PROVIDER_KEY, S3_ENV_PROVIDER);
                            Configuration hadoopConf =
                                    FileConfiguration.valueOf("S3")
                                            .getConfiguration()
                                            .buildConfiguration(config);

                            try (AWSCredentialProviderList providers =
                                    S3AUtils.createAWSCredentialProviderSet(
                                            URI.create("s3a://seatunnel-checkpoint"), hadoopConf)) {
                                AWSSessionCredentials credentials =
                                        Assertions.assertInstanceOf(
                                                AWSSessionCredentials.class,
                                                providers.getCredentials());

                                Assertions.assertEquals(
                                        S3_ACCESS_KEY, credentials.getAWSAccessKeyId());
                                Assertions.assertEquals(
                                        S3_SECRET_KEY, credentials.getAWSSecretKey());
                                Assertions.assertEquals(
                                        S3_SESSION_TOKEN, credentials.getSessionToken());
                            }
                        });
    }

    @Test
    public void testOssEnvironmentProviderReadsConfiguredValues() throws Exception {
        withEnvironmentVariable("OSS_ACCESS_KEY_ID", OSS_ACCESS_KEY)
                .and("OSS_ACCESS_KEY_SECRET", OSS_SECRET_KEY)
                .and("OSS_SESSION_TOKEN", OSS_SESSION_TOKEN)
                .execute(
                        () -> {
                            Map<String, String> config = new HashMap<>();
                            config.put("oss.bucket", "oss://seatunnel-checkpoint");
                            config.put("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
                            config.put(OSS_PROVIDER_KEY, OSS_ENV_PROVIDER);
                            Configuration hadoopConf =
                                    FileConfiguration.valueOf("OSS")
                                            .getConfiguration()
                                            .buildConfiguration(config);

                            CredentialsProvider provider =
                                    AliyunOSSUtils.getCredentialsProvider(
                                            URI.create("oss://seatunnel-checkpoint"), hadoopConf);
                            Assertions.assertInstanceOf(
                                    EnvironmentVariableCredentialsProvider.class, provider);
                            Credentials credentials = provider.getCredentials();

                            Assertions.assertEquals(OSS_ACCESS_KEY, credentials.getAccessKeyId());
                            Assertions.assertEquals(
                                    OSS_SECRET_KEY, credentials.getSecretAccessKey());
                            Assertions.assertEquals(
                                    OSS_SESSION_TOKEN, credentials.getSecurityToken());
                        });
    }

    @Test
    public void testCosEnvironmentProviderReadsConfiguredValues() throws Exception {
        withEnvironmentVariable("COSN_SECRET_ID", COS_SECRET_ID)
                .and("COSN_SECRET_KEY", COS_SECRET_KEY)
                .execute(
                        () -> {
                            Map<String, String> config = new HashMap<>();
                            config.put("cos.bucket", "cosn://seatunnel-checkpoint");
                            config.put("fs.cosn.bucket.region", "ap-shanghai");
                            config.put(COS_PROVIDER_KEY, COS_ENV_PROVIDER);
                            Configuration hadoopConf =
                                    FileConfiguration.valueOf("COS")
                                            .getConfiguration()
                                            .buildConfiguration(config);

                            COSCredentials credentials =
                                    CosNUtils.createCosCredentialsProviderSet(
                                                    URI.create("cosn://seatunnel-checkpoint"),
                                                    hadoopConf)
                                            .getCredentials();

                            Assertions.assertEquals(COS_SECRET_ID, credentials.getCOSAccessKeyId());
                            Assertions.assertEquals(COS_SECRET_KEY, credentials.getCOSSecretKey());
                        });
    }
}
