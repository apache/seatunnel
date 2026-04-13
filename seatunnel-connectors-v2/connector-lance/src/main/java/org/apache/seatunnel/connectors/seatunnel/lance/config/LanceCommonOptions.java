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

import java.util.ArrayList;
import java.util.List;

public class LanceCommonOptions {

    public static final Option<String> KEY_DATASET_PATH =
            Options.key("dataset_path")
                    .stringType()
                    .defaultValue("/test.lance")
                    .withDescription(" the lance dataset path");

    public static final Option<String> KEY_NAMESPACE_TYPE =
            Options.key("namespace_type")
                    .stringType()
                    .defaultValue("dir")
                    .withDescription(" the lance namespace type");

    public static final Option<List<String>> KEY_NAMESPACE_IDS =
            Options.key("namespace_ids")
                    .listType(String.class)
                    .defaultValue(new ArrayList<>())
                    .withDescription(" the lance namespace ids");

    public static final Option<String> KEY_NAMESPACE_ID =
            Options.key("namespace_id")
                    .stringType()
                    .defaultValue("")
                    .withDescription(" the lance namespace name");

    public static final Option<String> KEY_TABLE =
            Options.key("table")
                    .stringType()
                    .defaultValue("test")
                    .withDescription(" the lance table");

    public static final Option<String> KEY_ROOT_NAMESPACE_PATH =
            Options.key("root_namespace_path")
                    .stringType()
                    .defaultValue("/tmp")
                    .withDescription(" the lance root namespace path");
}
