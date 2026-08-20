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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Verifies that JDBC checkpoint state preserves XA transaction identifiers. */
class XidImplTest {

    @Test
    void testSerializeJdbcSinkStateWithXid() throws Exception {
        XidImpl xid = new XidImpl(42, new byte[] {1, 2, 3}, new byte[] {4, 5, 6});
        DefaultSerializer<JdbcSinkState> serializer = new DefaultSerializer<>();

        JdbcSinkState restored =
                serializer.deserialize(serializer.serialize(new JdbcSinkState(xid)));

        Assertions.assertEquals(xid, restored.getXid());
    }
}
