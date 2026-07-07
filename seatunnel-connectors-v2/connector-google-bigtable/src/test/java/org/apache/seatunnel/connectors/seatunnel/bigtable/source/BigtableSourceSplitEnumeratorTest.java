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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BigtableSourceSplitEnumeratorTest {

    @Test
    void testSingleSplitFullRange() {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "", "");
        assertEquals("bigtable_source_split_0", split.splitId());
        assertEquals("", split.getStartRowKey());
        assertEquals("", split.getEndRowKey());
    }

    @Test
    void testSplitWithRowKeyRange() {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "aaa", "zzz");
        assertEquals("aaa", split.getStartRowKey());
        assertEquals("zzz", split.getEndRowKey());
    }
}
