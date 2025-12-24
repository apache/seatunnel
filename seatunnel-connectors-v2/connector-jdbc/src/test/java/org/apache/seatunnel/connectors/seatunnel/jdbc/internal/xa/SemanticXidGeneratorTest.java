/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

class SemanticXidGeneratorTest {
    private SemanticXidGenerator xidGenerator;

    @BeforeEach
    void before() {
        xidGenerator = new SemanticXidGenerator();
        xidGenerator.open();
    }

    @Test
    void testBelongsToSubtask() {
        JobContext uuidJobContext = new JobContext();
        check(uuidJobContext);
        JobContext longJobContext = new JobContext(Long.MIN_VALUE);
        check(longJobContext);
    }

    void check(JobContext jobContext) {
        DefaultSinkWriterContext dc1 = new DefaultSinkWriterContext(Integer.MAX_VALUE, 1);
        Xid xid1 = xidGenerator.generateXid(jobContext, dc1, System.currentTimeMillis());
        Assertions.assertTrue(xidGenerator.belongsToSubtask(xid1, jobContext, dc1));
        Assertions.assertFalse(
                xidGenerator.belongsToSubtask(
                        xid1, jobContext, new DefaultSinkWriterContext(2, 1)));
        Assertions.assertFalse(xidGenerator.belongsToSubtask(xid1, new JobContext(), dc1));
    }
}
