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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;

import java.util.Map;

public class S3FileBaseOptions extends FileBaseSourceOptions {
    public static final Option<String> S3_ACCESS_KEY =
            Options.key("access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3 access key");
    public static final Option<String> S3_SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3 secret key");
    public static final Option<String> S3_BUCKET =
            Options.key("bucket").stringType().noDefaultValue().withDescription("S3 bucket");
    public static final Option<String> FS_S3A_ENDPOINT =
            Options.key("fs.s3a.endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("fs s3a endpoint");

    /**
     * The class name of the {@code SimpleAWSCredentialsProvider}, which authenticates with a static
     * {@code access_key} / {@code secret_key} pair.
     */
    public static final String SIMPLE_AWS_CREDENTIALS_PROVIDER =
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";

    /**
     * The class name of the {@code InstanceProfileCredentialsProvider}, which resolves credentials
     * from the runtime environment (for example an EC2 instance profile).
     */
    public static final String INSTANCE_PROFILE_CREDENTIALS_PROVIDER =
            "com.amazonaws.auth.InstanceProfileCredentialsProvider";

    /**
     * The S3A credentials provider class name passed through to Hadoop. Any fully-qualified S3A
     * credentials provider class available on the classpath is accepted, so container-oriented
     * providers (for example {@code com.amazonaws.auth.ContainerCredentialsProvider}) and custom
     * providers can be used in addition to the two well-known values {@link
     * #SIMPLE_AWS_CREDENTIALS_PROVIDER} and {@link #INSTANCE_PROFILE_CREDENTIALS_PROVIDER}. Hadoop
     * also accepts a comma-separated chain of provider classes. The class must be present on the
     * runtime classpath of every node running the S3A filesystem (for example under {@code
     * ${SEATUNNEL_HOME}/lib}).
     */
    public static final Option<String> S3A_AWS_CREDENTIALS_PROVIDER =
            Options.key("fs.s3a.aws.credentials.provider")
                    .stringType()
                    .defaultValue(INSTANCE_PROFILE_CREDENTIALS_PROVIDER)
                    .withDescription(
                            "The fully-qualified class name of the S3A credentials provider. "
                                    + "Defaults to "
                                    + INSTANCE_PROFILE_CREDENTIALS_PROVIDER
                                    + ". The class must be present on the classpath.");

    /**
     * The current key for that config option. if you need to add a new option, you can add it here
     * and refer to this:
     *
     * <p>https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
     *
     * <p>such as: key = "fs.s3a.session.token" value = "SECRET-SESSION-TOKEN"
     */
    public static final Option<Map<String, String>> S3_PROPERTIES =
            Options.key("hadoop_s3_properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("S3 properties");

    /**
     * The set of S3A credentials providers that used to be the only accepted values for {@link
     * #S3A_AWS_CREDENTIALS_PROVIDER}.
     *
     * @deprecated {@link #S3A_AWS_CREDENTIALS_PROVIDER} is now a free-form {@code String} option
     *     that accepts any S3A credentials provider class on the classpath. This enum is retained
     *     only for binary compatibility with downstream code compiled against earlier releases; use
     *     the {@link #SIMPLE_AWS_CREDENTIALS_PROVIDER} and {@link
     *     #INSTANCE_PROFILE_CREDENTIALS_PROVIDER} class-name constants instead.
     */
    @Deprecated
    public enum S3aAwsCredentialsProvider {
        SimpleAWSCredentialsProvider(SIMPLE_AWS_CREDENTIALS_PROVIDER),

        InstanceProfileCredentialsProvider(INSTANCE_PROFILE_CREDENTIALS_PROVIDER);

        private final String provider;

        S3aAwsCredentialsProvider(String provider) {
            this.provider = provider;
        }

        public String getProvider() {
            return provider;
        }

        @Override
        public String toString() {
            return provider;
        }
    }
}
