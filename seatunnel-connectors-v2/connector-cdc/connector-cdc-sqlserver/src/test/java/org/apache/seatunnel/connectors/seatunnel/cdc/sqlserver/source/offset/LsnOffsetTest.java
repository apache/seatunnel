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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.offset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.sqlserver.Lsn;

class LsnOffsetTest {

    @Test
    void testInitialOffsetRepresentsNoLsn() {
        LsnOffset initial = LsnOffset.INITIAL_OFFSET;

        // no LSN keys should be present in the offset map
        Assertions.assertTrue(initial.getOffset().isEmpty());

        // commit LSN resolved from the empty map should be Debezium's NULL LSN
        Lsn commitLsn = initial.getCommitLsn();
        Assertions.assertFalse(commitLsn.isAvailable());
    }
}
