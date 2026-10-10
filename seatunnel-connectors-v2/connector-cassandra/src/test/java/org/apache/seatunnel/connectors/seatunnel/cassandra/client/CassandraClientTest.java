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

package org.apache.seatunnel.connectors.seatunnel.cassandra.client;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class CassandraClientTest {
    @Test
    void registersAllContactPointsOnOneBuilder() {
        assertContactPoints("127.0.0.1:9042,127.0.0.2:9043,127.0.0.3:9044", "", "");
    }

    @Test
    void preservesCredentialsWithMultipleContactPoints() {
        assertContactPoints("127.0.0.1:9042,127.0.0.2:9043", "test-user", "test-password");
    }

    @Test
    void preservesSingleContactPoint() {
        assertContactPoints("127.0.0.1:9042", "", "");
    }

    @Test
    void passesDuplicateContactPointsToTheSameBuilder() {
        CqlSessionBuilder builder = mock(CqlSessionBuilder.class, RETURNS_SELF);
        try (MockedStatic<CqlSession> factory = Mockito.mockStatic(CqlSession.class)) {
            factory.when(CqlSession::builder).thenReturn(builder);
            assertSame(
                    builder,
                    CassandraClient.getCqlSessionBuilder(
                            "127.0.0.1:9042,127.0.0.1:9042", "test", "", "", "datacenter1"));
            factory.verify(CqlSession::builder, times(1));
            verify(builder, times(2)).addContactPoint(new InetSocketAddress("127.0.0.1", 9042));
        }
    }

    private void assertContactPoints(String hosts, String user, String password) {
        CqlSessionBuilder builder = mock(CqlSessionBuilder.class, RETURNS_SELF);
        try (MockedStatic<CqlSession> factory = Mockito.mockStatic(CqlSession.class)) {
            factory.when(CqlSession::builder).thenReturn(builder);
            assertSame(
                    builder,
                    CassandraClient.getCqlSessionBuilder(
                            hosts, "test", user, password, "datacenter1"));
            factory.verify(CqlSession::builder, times(1));
            for (String host : hosts.split(",")) {
                String[] address = host.split(":");
                verify(builder)
                        .addContactPoint(
                                new InetSocketAddress(address[0], Integer.parseInt(address[1])));
            }
            verify(builder).withKeyspace("test");
            verify(builder).withLocalDatacenter("datacenter1");
            if (user.isEmpty() && password.isEmpty()) {
                verify(builder, never())
                        .withAuthCredentials(Mockito.anyString(), Mockito.anyString());
            } else {
                verify(builder).withAuthCredentials(user, password);
            }
            verify(builder, never()).build();
        }
    }
}
