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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

public class S3HadoopConf extends HadoopConf {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3HadoopConf.class);
    private static final String HDFS_S3N_IMPL = "org.apache.hadoop.fs.s3native.NativeS3FileSystem";
    private static final String HDFS_S3A_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem";
    protected static final String S3A_SCHEMA = "s3a";
    protected static final String DEFAULT_SCHEMA = "s3n";
    private String schema = DEFAULT_SCHEMA;

    @Override
    public String getFsHdfsImpl() {
        return switchHdfsImpl();
    }

    @Override
    public String getSchema() {
        return this.schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public S3HadoopConf(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    public static HadoopConf buildWithReadOnlyConfig(ReadonlyConfig config) {

        String bucketName = config.get(S3FileBaseOptions.S3_BUCKET);
        S3HadoopConf hadoopConf = new S3HadoopConf(bucketName);
        if (bucketName.startsWith(S3A_SCHEMA)) {
            hadoopConf.setSchema(S3A_SCHEMA);
        }
        HashMap<String, String> s3Options = new HashMap<>();
        hadoopConf.putS3SK(s3Options, config);
        if (config.getOptional(S3FileBaseOptions.S3_PROPERTIES).isPresent()) {
            config.get(S3FileBaseOptions.S3_PROPERTIES)
                    .forEach((key, value) -> s3Options.put(key, String.valueOf(value)));
        }

        String credentialsProvider = config.get(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER);
        checkCredentialsProviders(credentialsProvider);
        s3Options.put(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER.key(), credentialsProvider);
        s3Options.put(
                S3FileBaseOptions.FS_S3A_ENDPOINT.key(),
                config.get(S3FileBaseOptions.FS_S3A_ENDPOINT));
        hadoopConf.setExtraOptions(s3Options);
        return hadoopConf;
    }

    protected String switchHdfsImpl() {
        switch (this.schema) {
            case S3A_SCHEMA:
                return HDFS_S3A_IMPL;
            default:
                return HDFS_S3N_IMPL;
        }
    }

    private void putS3SK(Map<String, String> s3Options, ReadonlyConfig config) {
        if (!config.getOptional(S3FileBaseOptions.S3_ACCESS_KEY).isPresent()
                && !config.getOptional(S3FileBaseOptions.S3_SECRET_KEY).isPresent()) {
            return;
        }
        String accessKey = config.get(S3FileBaseOptions.S3_ACCESS_KEY);
        String secretKey = config.get(S3FileBaseOptions.S3_SECRET_KEY);
        if (S3A_SCHEMA.equals(this.schema)) {
            s3Options.put("fs.s3a.access.key", accessKey);
            s3Options.put("fs.s3a.secret.key", secretKey);
            return;
        }
        // default s3n
        s3Options.put("fs.s3n.awsAccessKeyId", accessKey);
        s3Options.put("fs.s3n.awsSecretAccessKey", secretKey);
    }

    /**
     * The AWS credentials provider contract every configured provider class must implement. Hadoop
     * S3A's {@code S3AUtils.createAWSCredentialProvider} performs the exact same {@code
     * AWSCredentialsProvider.isAssignableFrom(clazz)} check (and rejects abstract classes) before
     * it reflectively instantiates the class, so validating it here only moves that failure
     * earlier, from an opaque worker-side {@code IOException} to an actionable config-parse-time
     * error. It never rejects anything Hadoop itself would have accepted.
     */
    private static final String AWS_CREDENTIALS_PROVIDER_INTERFACE =
            "com.amazonaws.auth.AWSCredentialsProvider";

    /**
     * Best-effort eager validation of the configured S3A credentials provider value on the node
     * that builds the Hadoop configuration. Hadoop accepts a comma-separated chain of provider
     * classes, so each entry is validated independently.
     *
     * <p>The splitting mirrors Hadoop's own {@code getTrimmedStrings} semantics: entries are
     * separated by commas and surrounding whitespace is trimmed, so a value such as {@code "A, B"}
     * that Hadoop accepts is accepted here too. An empty entry (for example {@code "A,,B"} or a
     * trailing comma) is rejected, because Hadoop would treat it as a blank class name and fail at
     * runtime with a far less actionable error.
     *
     * <p>The provider is ultimately instantiated on every worker by Hadoop S3A, and the classpath
     * of the node building this configuration (client/coordinator, or a module embedding this
     * connector) may legitimately differ from the workers' — the provider jar only has to be
     * present where S3A actually runs (for example under {@code ${SEATUNNEL_HOME}/lib} on every
     * cluster node). A class that cannot be resolved here is therefore only logged as a warning,
     * never rejected. Only a class that resolves but does not implement the AWS credentials
     * provider contract, or a malformed (empty) chain entry, fails fast because that configuration
     * cannot work on any node.
     *
     * @param credentialsProvider the raw option value (single class or comma-separated chain)
     */
    private static void checkCredentialsProviders(String credentialsProvider) {
        if (credentialsProvider == null || credentialsProvider.trim().isEmpty()) {
            return;
        }
        // Keep trailing empty segments (split limit -1) so that a stray comma such as "A," is
        // detected instead of being silently dropped.
        String[] providerClassNames = credentialsProvider.split(",", -1);
        for (String providerClassName : providerClassNames) {
            String trimmed = providerClassName.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format(
                                "The S3A credentials provider chain configured via '%s' contains an "
                                        + "empty class name ('%s'). Please remove the stray comma so "
                                        + "every entry is a fully-qualified provider class name.",
                                S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER.key(),
                                credentialsProvider));
            }
            checkCredentialsProviderClass(trimmed);
        }
    }

    /**
     * Validates a single S3A credentials provider class as far as the local classpath allows.
     *
     * <p>The lookup tries the thread context classloader first, then falls back to the classloader
     * that loaded this connector. Under the Zeta plugin classloader the context classloader may be
     * {@code null} or unrelated to the connector, so the fallback ensures a provider bundled with
     * the connector (including the default {@link
     * S3FileBaseOptions#INSTANCE_PROFILE_CREDENTIALS_PROVIDER}) is not rejected by mistake.
     *
     * @param providerClassName the fully-qualified credentials provider class name
     */
    private static void checkCredentialsProviderClass(String providerClassName) {
        for (ClassLoader classLoader : credentialsProviderClassLoaders()) {
            if (classLoader == null) {
                continue;
            }
            Class<?> providerClass;
            try {
                providerClass = Class.forName(providerClassName, false, classLoader);
            } catch (ClassNotFoundException ignored) {
                // Try the next classloader before concluding the class is not visible here.
                continue;
            }
            assertImplementsCredentialsProvider(providerClass, classLoader);
            return;
        }
        LOGGER.warn(
                "The S3A credentials provider class '{}' configured via '{}' is not visible on "
                        + "this node's classpath. The job will fail at runtime unless the provider "
                        + "jar is available on every node running the S3A filesystem (for example "
                        + "under ${SEATUNNEL_HOME}/lib).",
                providerClassName,
                S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER.key());
    }

    /**
     * Asserts the resolved provider class can actually be used by Hadoop S3A: it must implement
     * {@link #AWS_CREDENTIALS_PROVIDER_INTERFACE} and must not be abstract. These are exactly the
     * two conditions {@code S3AUtils.createAWSCredentialProvider} enforces before instantiation, so
     * this check never rejects a class Hadoop would have accepted — it only surfaces the failure at
     * config-parse time with an actionable message instead of an opaque worker-side error.
     *
     * <p>The assertion is only enforced when the interface itself resolves from the same
     * classloader that loaded the provider class; otherwise it is skipped. This avoids falsely
     * rejecting a valid provider when the AWS SDK interface is not visible from the classloader
     * that happened to resolve the provider (for example under plugin classloader isolation),
     * trading a missed check for never blocking a legitimate configuration.
     */
    private static void assertImplementsCredentialsProvider(
            Class<?> providerClass, ClassLoader classLoader) {
        Class<?> providerInterface;
        try {
            providerInterface =
                    Class.forName(AWS_CREDENTIALS_PROVIDER_INTERFACE, false, classLoader);
        } catch (ClassNotFoundException ignored) {
            // The AWS SDK interface is not visible from this classloader; skip the assertion
            // rather than risk rejecting a legitimate provider.
            return;
        }
        if (!providerInterface.isAssignableFrom(providerClass)) {
            throw new IllegalArgumentException(
                    String.format(
                            "The S3A credentials provider class '%s' does not implement '%s'. "
                                    + "Please check the value of '%s' and configure a class that "
                                    + "implements the AWS credentials provider contract.",
                            providerClass.getName(),
                            AWS_CREDENTIALS_PROVIDER_INTERFACE,
                            S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER.key()));
        }
        if (Modifier.isAbstract(providerClass.getModifiers())) {
            throw new IllegalArgumentException(
                    String.format(
                            "The S3A credentials provider class '%s' configured via '%s' is "
                                    + "abstract and cannot be instantiated. Please configure a "
                                    + "concrete AWS credentials provider class.",
                            providerClass.getName(),
                            S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER.key()));
        }
    }

    private static ClassLoader[] credentialsProviderClassLoaders() {
        return new ClassLoader[] {
            Thread.currentThread().getContextClassLoader(), S3HadoopConf.class.getClassLoader()
        };
    }
}
