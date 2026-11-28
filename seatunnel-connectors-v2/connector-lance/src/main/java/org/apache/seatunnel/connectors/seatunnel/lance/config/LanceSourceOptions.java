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

package org.apache.seatunnel.connectors.seatunnel.lance.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.HashMap;
import java.util.Map;

public class LanceSourceOptions extends LanceCommonOptions {

    public static final Option<Map<String, String>> READ_STORAGE_OPTIONS =
            Options.key("lance.read.storage.options")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "Storage options used when opening the dataset for reading. Keys are the same as"
                                    + " the sink's lance.write.storage.options (e.g. aws_endpoint_url,"
                                    + " aws_access_key_id, aws_secret_access_key, allow_http).");
}
