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
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;

import java.util.Map;

public class S3ConfigOptions extends BaseSourceConfigOptions {
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

    public static final Option<S3aAwsCredentialsProvider> S3A_AWS_CREDENTIALS_PROVIDER =
            Options.key("fs.s3a.aws.credentials.provider")
                    .enumType(S3aAwsCredentialsProvider.class)
                    .defaultValue(S3aAwsCredentialsProvider.InstanceProfileCredentialsProvider)
                    .withDescription("s3a aws credentials provider");

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

    public enum S3aAwsCredentialsProvider {
        SimpleAWSCredentialsProvider("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),

        InstanceProfileCredentialsProvider("com.amazonaws.auth.InstanceProfileCredentialsProvider");

        private String provider;

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
