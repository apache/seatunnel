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

package org.apache.seatunnel.connectors.seatunnel.jdbc.state;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

import java.lang.reflect.Constructor;
import java.util.Arrays;

class JdbcSinkStateTest {

    @Test
    void testSerializeEvolvedTableSchema() throws Exception {
        TableSchema evolvedSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, (Long) null, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "email",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .build();
        DefaultSerializer<JdbcSinkState> serializer = new DefaultSerializer<>();

        JdbcSinkState restored =
                serializer.deserialize(
                        serializer.serialize(new JdbcSinkState(null, evolvedSchema)));

        Assertions.assertEquals(evolvedSchema, restored.getTableSchema());
        Assertions.assertNull(restored.getXid());
    }

    @Test
    void testStateWithoutSchemaSupportsInitialSchemaFallback() {
        JdbcSinkState state = new JdbcSinkState(null);

        Assertions.assertNull(state.getTableSchema());
    }

    @Test
    void testSerializeStateWithRecoveredXid() throws Exception {
        TableSchema evolvedSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, (Long) null, false, null, null))
                        .build();
        Xid recoveredXid = createXid(201, new byte[] {1, 2, 3}, new byte[] {4, 5});
        DefaultSerializer<JdbcSinkState> serializer = new DefaultSerializer<>();

        JdbcSinkState restored =
                serializer.deserialize(
                        serializer.serialize(new JdbcSinkState(recoveredXid, evolvedSchema)));

        Assertions.assertEquals(recoveredXid, restored.getXid());
        Assertions.assertEquals(evolvedSchema, restored.getTableSchema());
        Assertions.assertArrayEquals(
                recoveredXid.getGlobalTransactionId(), restored.getXid().getGlobalTransactionId());
        Assertions.assertArrayEquals(
                recoveredXid.getBranchQualifier(), restored.getXid().getBranchQualifier());
    }

    private static Xid createXid(int formatId, byte[] gtrid, byte[] bqual) throws Exception {
        Class<?> xidImplClass =
                Class.forName("org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XidImpl");
        Constructor<?> constructor =
                xidImplClass.getDeclaredConstructor(int.class, byte[].class, byte[].class);
        constructor.setAccessible(true);
        return (Xid)
                constructor.newInstance(
                        formatId,
                        Arrays.copyOf(gtrid, gtrid.length),
                        Arrays.copyOf(bqual, bqual.length));
    }
}
