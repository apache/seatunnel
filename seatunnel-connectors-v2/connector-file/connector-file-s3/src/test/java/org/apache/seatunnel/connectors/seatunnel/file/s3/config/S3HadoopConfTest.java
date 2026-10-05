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

package org.apache.seatunnel.connectors.seatunnel.file.s3.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class S3HadoopConfTest {

    @Test
    void testPutS3SK() {
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put("access_key", "access_key");
        config.put("secret_key", "secret_key");
        HadoopConf conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertTrue(conf.getExtraOptions().containsKey("fs.s3n.awsAccessKeyId"));

        config.remove("access_key");
        conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertTrue(conf.getExtraOptions().containsKey("fs.s3n.awsAccessKeyId"));

        config.remove("secret_key");
        conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertFalse(conf.getExtraOptions().containsKey("fs.s3n.awsAccessKeyId"));
    }

    @Test
    void testDefaultCredentialsProvider() {
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        HadoopConf conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertEquals(
                S3FileBaseOptions.INSTANCE_PROFILE_CREDENTIALS_PROVIDER,
                conf.getExtraOptions()
                        .get(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key()));
    }

    @Test
    void testLegacyCredentialsProviderOptionTypeIsPreserved() {
        Map<String, Object> config = new HashMap<>();
        config.put(
                S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER.key(),
                S3FileBaseOptions.SIMPLE_AWS_CREDENTIALS_PROVIDER);

        S3FileBaseOptions.S3aAwsCredentialsProvider provider =
                ReadonlyConfig.fromMap(config).get(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER);

        Assertions.assertEquals(
                S3FileBaseOptions.S3aAwsCredentialsProvider.SimpleAWSCredentialsProvider, provider);
    }

    @Test
    void testCustomCredentialsProviderIsPassedThrough() {
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put(
                S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(),
                S3FileBaseOptions.SIMPLE_AWS_CREDENTIALS_PROVIDER);
        config.put("access_key", "access_key");
        config.put("secret_key", "secret_key");
        HadoopConf conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertEquals(
                S3FileBaseOptions.SIMPLE_AWS_CREDENTIALS_PROVIDER,
                conf.getExtraOptions()
                        .get(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key()));
    }

    @Test
    void testUnknownCredentialsProviderClassIsPassedThroughWithWarning() {
        // A class that is not resolvable on this node may still exist on the worker classpath
        // where Hadoop S3A actually instantiates it, so it must pass through instead of failing.
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put(
                S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(),
                "com.example.NonExistentCredentialsProvider");
        HadoopConf conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertEquals(
                "com.example.NonExistentCredentialsProvider",
                conf.getExtraOptions()
                        .get(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key()));
    }

    @Test
    void testNonCredentialsProviderClassFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(), "java.lang.String");
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(exception.getMessage().contains("does not implement"));
    }

    @Test
    void testNonProviderCannotBypassValidationWithIsolatedContextClassLoader() {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(new ClassLoader(null) {});
            Map<String, Object> config = new HashMap<>();
            config.put("bucket", "test");
            config.put(
                    S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(), "java.lang.String");

            Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config)));
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void testProviderChainIsPassedThrough() {
        String chain =
                S3FileBaseOptions.SIMPLE_AWS_CREDENTIALS_PROVIDER
                        + ","
                        + S3FileBaseOptions.INSTANCE_PROFILE_CREDENTIALS_PROVIDER;
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(), chain);
        config.put("access_key", "access_key");
        config.put("secret_key", "secret_key");
        HadoopConf conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertEquals(
                chain,
                conf.getExtraOptions()
                        .get(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key()));
    }

    @Test
    void testProviderChainWithNonProviderClassFails() {
        // Chain entries are validated independently; a resolvable class that is not a credentials
        // provider still fails fast because it cannot work on any node.
        String chain = S3FileBaseOptions.SIMPLE_AWS_CREDENTIALS_PROVIDER + ",java.lang.String";
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(), chain);
        config.put("access_key", "access_key");
        config.put("secret_key", "secret_key");
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(exception.getMessage().contains("does not implement"));
    }

    @Test
    void testProviderChainWithWhitespaceIsPassedThrough() {
        // Hadoop trims whitespace around commas, so "A, B" is a valid chain; the raw value
        // (including the space) is passed through unchanged for Hadoop to parse.
        String chain =
                S3FileBaseOptions.SIMPLE_AWS_CREDENTIALS_PROVIDER
                        + ", "
                        + S3FileBaseOptions.INSTANCE_PROFILE_CREDENTIALS_PROVIDER;
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(), chain);
        config.put("access_key", "access_key");
        config.put("secret_key", "secret_key");
        HadoopConf conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertEquals(
                chain,
                conf.getExtraOptions()
                        .get(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key()));
    }

    @Test
    void testProviderChainWithNewlineValidatesEachEntry() {
        String chain = S3FileBaseOptions.SIMPLE_AWS_CREDENTIALS_PROVIDER + "\njava.lang.String";
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(), chain);
        config.put("access_key", "access_key");
        config.put("secret_key", "secret_key");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(exception.getMessage().contains("does not implement"));
    }

    @Test
    void testProviderChainWithEmptySegmentFails() {
        String chain =
                S3FileBaseOptions.SIMPLE_AWS_CREDENTIALS_PROVIDER
                        + ",,"
                        + S3FileBaseOptions.INSTANCE_PROFILE_CREDENTIALS_PROVIDER;
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(), chain);
        config.put("access_key", "access_key");
        config.put("secret_key", "secret_key");
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(exception.getMessage().contains("empty class name"));
    }

    @Test
    void testProviderChainWithTrailingCommaMatchesHadoopSemantics() {
        String chain = S3FileBaseOptions.SIMPLE_AWS_CREDENTIALS_PROVIDER + ",";
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(), chain);
        config.put("access_key", "access_key");
        config.put("secret_key", "secret_key");

        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration(false);
        hadoopConfig.set(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(), chain);
        Assertions.assertEquals(
                1,
                hadoopConfig.getClasses(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key())
                        .length);

        HadoopConf conf = S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config));
        Assertions.assertEquals(
                chain,
                conf.getExtraOptions()
                        .get(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key()));
    }

    @Test
    void testAbstractCredentialsProviderClassFails() {
        // An abstract class implementing the provider contract passes the isAssignableFrom check
        // but cannot be instantiated by Hadoop S3A, which rejects it before construction; the
        // eager check mirrors that and fails fast with an actionable message.
        Map<String, Object> config = new HashMap<>();
        config.put("bucket", "test");
        config.put(
                S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(),
                AbstractTestCredentialsProvider.class.getName());
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> S3HadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(config)));
        Assertions.assertTrue(exception.getMessage().contains("abstract"));
    }

    /**
     * An abstract class that implements the AWS credentials provider contract. Used to verify the
     * eager validation rejects abstract classes, matching Hadoop {@code S3AUtils} behavior.
     */
    abstract static class AbstractTestCredentialsProvider
            implements com.amazonaws.auth.AWSCredentialsProvider {}
}
