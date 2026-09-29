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

package org.apache.seatunnel.api.metadata;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/** Configuration options for Metadata Center. */
public class MetadataOptions {

    /** The key for metadata configuration in env config. */
    public static final Option<String> METADATA =
            Options.key("metadata")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The metadata configuration containing enabled, kind and provider-specific properties.");

    /** Whether to enable Metadata Center. */
    public static final Option<Boolean> ENABLED =
            Options.key("enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable Metadata Center for centralized data source metadata management. "
                                    + "When enabled, data source connection details can be referenced via datasourceId instead of being directly specified in job configs.");

    /**
     * The kind of Metadata provider to use. Supported values: "gravitino", "datahub", "atlas", etc.
     */
    public static final Option<String> KIND =
            Options.key("kind")
                    .stringType()
                    .defaultValue("gravitino")
                    .withDescription(
                            "The kind of Metadata provider to use. Supported values: gravitino, datahub, atlas, etc.");
}
