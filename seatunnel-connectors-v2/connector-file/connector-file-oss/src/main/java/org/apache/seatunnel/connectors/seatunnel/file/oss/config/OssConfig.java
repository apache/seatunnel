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

package org.apache.seatunnel.connectors.seatunnel.file.oss.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;

public class OssConfig extends BaseSourceConfig {
    public static final Option<String> ACCESS_KEY =
            Options.key("access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OSS bucket access key");
    public static final Option<String> ACCESS_SECRET =
            Options.key("access_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OSS bucket access secret");
    public static final Option<String> ENDPOINT =
            Options.key("endpoint").stringType().noDefaultValue().withDescription("OSS endpoint");
    public static final Option<String> BUCKET =
            Options.key("bucket").stringType().noDefaultValue().withDescription("OSS bucket");
}
