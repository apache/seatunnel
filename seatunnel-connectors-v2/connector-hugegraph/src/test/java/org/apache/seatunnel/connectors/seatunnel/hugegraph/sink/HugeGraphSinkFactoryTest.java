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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkOptions;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class HugeGraphSinkFactoryTest {

    /**
     * Every option the sink actually reads must be declared in {@code optionRule()}; otherwise
     * {@code seatunnel.sh --config x --check} (and STATIC dry-run) reject it as an unknown key and
     * it is invisible to option-listing tooling.
     */
    @Test
    void optionRuleDeclaresAllReadOptions() {
        List<Option<?>> optional = new HugeGraphSinkFactory().optionRule().getOptionalOptions();
        assertTrue(
                optional.contains(HugeGraphSinkOptions.DATA_SAVE_MODE), "data_save_mode missing");
        assertTrue(optional.contains(HugeGraphOptions.CHECK_VERTEX), "check_vertex missing");
        assertTrue(
                optional.contains(HugeGraphOptions.BATCH_FAILURE_FALLBACK),
                "batch_failure_fallback missing");
        assertTrue(
                optional.contains(HugeGraphOptions.MAX_INSERT_ERRORS), "max_insert_errors missing");
        assertTrue(
                optional.contains(HugeGraphOptions.FAILURE_DATA_PATH), "failure_data_path missing");
        assertTrue(
                optional.contains(HugeGraphOptions.RETRY_BACKOFF_MAX_MS),
                "retry_backoff_max_ms missing");
    }
}
