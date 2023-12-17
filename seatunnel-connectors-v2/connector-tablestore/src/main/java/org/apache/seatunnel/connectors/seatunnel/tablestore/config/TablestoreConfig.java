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

package org.apache.seatunnel.connectors.seatunnel.tablestore.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;

public class TablestoreConfig implements Serializable {
    public static final Option<String> END_POINT =
            Options.key("end_point")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(" Tablestore end_point");
    public static final Option<String> INSTANCE_NAME =
            Options.key("instance_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(" Tablestore instance_name");
    public static final Option<String> ACCESS_KEY_ID =
            Options.key("access_key_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(" Tablestore access_key_id");
    public static final Option<String> ACCESS_KEY_SECRET =
            Options.key("access_key_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(" Tablestore access_key_secret");
    public static final Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue().withDescription(" Tablestore table");
    public static final Option<String> BATCH_SIZE =
            Options.key("batch_size")
                    .stringType()
                    .defaultValue("25")
                    .withDescription(" Tablestore batch_size");
    public static final Option<String> PRIMARY_KEYS =
            Options.key("primary_keys")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(" Tablestore primary_keys");
}
