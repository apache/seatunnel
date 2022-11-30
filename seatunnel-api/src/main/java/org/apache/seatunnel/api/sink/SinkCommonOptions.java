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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class SinkCommonOptions {

    public static final Option<String> SOURCE_TABLE_NAME =
        Options.key("source_table_name")
            .stringType()
            .noDefaultValue()
            .withDescription(
                "When source_table_name is not specified, " +
                    "the current plug-in processes the data set dataset output by the previous plugin in the configuration file. " +
                    "When source_table_name is specified, the current plug-in is processing the data set corresponding to this parameter.");

    public static final Option<Integer> PARALLELISM =
        Options.key("parallelism")
            .intType()
            .noDefaultValue()
            .withDescription("When parallelism is not specified, the parallelism in env is used by default. " +
                "When parallelism is specified, it will override the parallelism in env.");
}
