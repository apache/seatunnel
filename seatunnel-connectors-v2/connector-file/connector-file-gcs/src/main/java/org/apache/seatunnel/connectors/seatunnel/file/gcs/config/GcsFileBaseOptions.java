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

package org.apache.seatunnel.connectors.seatunnel.file.gcs.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;

import java.util.Map;

public class GcsFileBaseOptions extends FileBaseSourceOptions {

    public static final Option<String> BUCKET =
            Options.key("bucket")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Google Cloud Storage bucket URI, for example gs://my-bucket");

    public static final Option<String> SERVICE_ACCOUNT_KEY_FILE =
            Options.key("service_account_key_file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Path to a service account JSON key file available on every worker. "
                                    + "When omitted, the GCS connector uses Application Default Credentials");

    public static final Option<Map<String, String>> GCS_PROPERTIES =
            Options.key("hadoop_gcs_properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Additional Hadoop GCS connector properties. Explicit connector "
                                    + "options take precedence over entries in this map");
}
