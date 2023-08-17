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

package org.apache.seatunnel.connectors.seatunnel.file.cos.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;

public class CosConfig extends BaseSourceConfig {
    public static final Option<String> SECRET_ID =
            Options.key("secret_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("COS bucket secret id");
    public static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("COS bucket secret key");
    public static final Option<String> REGION =
            Options.key("region").stringType().noDefaultValue().withDescription("COS region");
    public static final Option<String> BUCKET =
            Options.key("bucket").stringType().noDefaultValue().withDescription("COS bucket");
}
