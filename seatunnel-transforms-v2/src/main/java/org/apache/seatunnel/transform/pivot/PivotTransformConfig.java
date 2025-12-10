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

package org.apache.seatunnel.transform.pivot;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

/**
 * Configuration options for the Pivot (Row-to-Column) Transform.
 *
 * <p>This transform converts multiple rows into a single row by pivoting on a column.
 *
 * <p>Example:
 *
 * <pre>
 * Input:
 * | id | type | value |
 * |----|------|-------|
 * | 1  | A    | 100   |
 * | 1  | B    | 200   |
 * | 2  | A    | 150   |
 *
 * Output (pivot on 'type', value from 'value', group by 'id'):
 * | id | A   | B    |
 * |----|-----|------|
 * | 1  | 100 | 200  |
 * | 2  | 150 | null |
 * </pre>
 */
public class PivotTransformConfig {

    public static final String PLUGIN_NAME = "Pivot";

    /**
     * The columns used to group rows together. Rows with the same values in these columns will be
     * combined into a single output row.
     */
    public static final Option<List<String>> GROUP_BY_KEYS =
            Options.key("group_by_keys")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription(
                            "The columns used to group rows together. "
                                    + "Rows with the same values in these columns will be combined into a single output row.");

    /**
     * The column whose values will become new column names in the output. Each unique value in this
     * column creates a new column.
     */
    public static final Option<String> PIVOT_COLUMN =
            Options.key("pivot_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The column whose values will become new column names in the output. "
                                    + "Each unique value in this column creates a new column.");

    /** The column whose values will populate the new pivoted columns. */
    public static final Option<String> VALUE_COLUMN =
            Options.key("value_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The column whose values will populate the new pivoted columns.");

    /**
     * Optional: Pre-defined list of pivot values. If specified, only these values from pivot_column
     * will create new columns. If not specified, columns will be dynamically determined.
     */
    public static final Option<List<String>> PIVOT_VALUES =
            Options.key("pivot_values")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription(
                            "Optional: Pre-defined list of pivot values. "
                                    + "If specified, only these values from pivot_column will create new columns. "
                                    + "If not specified, columns must be pre-defined.");

    /** The default value to use when a pivot value is missing for a group. Defaults to null. */
    public static final Option<String> DEFAULT_VALUE =
            Options.key("default_value")
                    .stringType()
                    .defaultValue(null)
                    .withDescription(
                            "The default value to use when a pivot value is missing for a group.");

    /**
     * Maximum number of groups to buffer before forcing a flush. This helps control memory usage
     * for streaming scenarios.
     */
    public static final Option<Integer> MAX_BUFFER_SIZE =
            Options.key("max_buffer_size")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Maximum number of groups to buffer before forcing a flush. "
                                    + "Set to -1 for unlimited buffering (flush only on checkpoint).");

    /**
     * Timeout in milliseconds for a group. If a group hasn't received new data within this timeout,
     * it will be flushed. Set to -1 to disable timeout-based flushing.
     */
    public static final Option<Long> GROUP_TIMEOUT_MS =
            Options.key("group_timeout_ms")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "Timeout in milliseconds for a group. "
                                    + "If a group hasn't received new data within this timeout, it will be flushed. "
                                    + "Set to -1 to disable timeout-based flushing.");
}
