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

package org.apache.seatunnel.api.options;

import org.apache.seatunnel.api.annotation.Experimental;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class MultiTableCommonOptions {

    @Experimental
    public static final Option<MultiTableFailurePolicy> MULTI_TABLE_FAILURE_POLICY =
            Options.key("multi_table.failure_policy")
                    .enumType(MultiTableFailurePolicy.class)
                    .defaultValue(MultiTableFailurePolicy.FAIL_FAST)
                    .withDescription(
                            "Failure handling policy for multi-table jobs. "
                                    + "FAIL_FAST aborts the whole job on the first table error. "
                                    + "CONTINUE_OTHER_TABLES isolates failed tables and keeps healthy tables running when the error can be attributed to a single table.");

    public static final Option<List<String>> MULTI_TABLE_INITIAL_FAILED_TABLES =
            Options.key("seatunnel.multi_table.initial_failed_tables")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Internal option used to propagate pre-runtime failed-table metadata into MultiTableSink.");

    private MultiTableCommonOptions() {}
}
