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

package org.apache.seatunnel.connectors.seatunnel.file.sftp.system;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests the SFTP connection pool bookkeeping that continuous discovery relies on during cleanup.
 */
class SFTPConnectionPoolTest {

    /**
     * Keep sibling idle channels tracked after one channel is borrowed from the shared pool key.
     */
    @Test
    void getFromPoolShouldKeepOtherIdleChannelsTracked() throws Exception {
        SFTPConnectionPool connectionPool = new SFTPConnectionPool(2, 0);
        SFTPConnectionPool.ConnectionInfo connectionInfo =
                new SFTPConnectionPool.ConnectionInfo("host", 22, "user");
        ChannelSftp firstChannel = new TestChannelSftp(true, Mockito.mock(Session.class));
        ChannelSftp secondChannel = new TestChannelSftp(true, Mockito.mock(Session.class));

        HashSet<ChannelSftp> idleChannels = new HashSet<>();
        idleChannels.add(firstChannel);
        idleChannels.add(secondChannel);
        idleConnections(connectionPool).put(connectionInfo, idleChannels);
        trackedConnections(connectionPool).put(firstChannel, connectionInfo);
        trackedConnections(connectionPool).put(secondChannel, connectionInfo);

        ChannelSftp borrowedChannel = connectionPool.getFromPool(connectionInfo);

        Assertions.assertTrue(borrowedChannel == firstChannel || borrowedChannel == secondChannel);
        Set<ChannelSftp> remainingIdleChannels =
                idleConnections(connectionPool).get(connectionInfo);
        Assertions.assertNotNull(remainingIdleChannels);
        Assertions.assertEquals(1, remainingIdleChannels.size());
        Assertions.assertFalse(remainingIdleChannels.contains(borrowedChannel));
    }

    /** Always disconnect the SSH session when a stale channel is being permanently closed. */
    @Test
    void disconnectShouldCloseSessionForDisconnectedChannel() throws Exception {
        SFTPConnectionPool connectionPool = new SFTPConnectionPool(0, 1);
        SFTPConnectionPool.ConnectionInfo connectionInfo =
                new SFTPConnectionPool.ConnectionInfo("host", 22, "user");
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.isConnected()).thenReturn(true);
        TestChannelSftp channel = new TestChannelSftp(false, session);
        trackedConnections(connectionPool).put(channel, connectionInfo);

        connectionPool.disconnect(channel);

        Mockito.verify(session).disconnect();
        Assertions.assertFalse(channel.wasDisconnected());
        Assertions.assertEquals(0, connectionPool.getLiveConnCount());
        Assertions.assertEquals(0, connectionPool.getConnPoolSize());
    }

    /** Shutdown must close tracked sessions even when live-count bookkeeping has drifted. */
    @Test
    void shutdownShouldCloseTrackedChannelsRegardlessOfLiveCount() throws Exception {
        SFTPConnectionPool connectionPool = new SFTPConnectionPool(2, 0);
        SFTPConnectionPool.ConnectionInfo connectionInfo =
                new SFTPConnectionPool.ConnectionInfo("host", 22, "user");
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.isConnected()).thenReturn(true);
        TestChannelSftp channel = new TestChannelSftp(true, session);
        trackedConnections(connectionPool).put(channel, connectionInfo);

        connectionPool.shutdown();

        Assertions.assertTrue(channel.wasDisconnected());
        Mockito.verify(session).disconnect();
    }

    /** Late disconnects after shutdown must close the channel instead of touching closed maps. */
    @Test
    void disconnectAfterShutdownShouldCloseChannel() throws Exception {
        SFTPConnectionPool connectionPool = new SFTPConnectionPool(1, 0);
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.isConnected()).thenReturn(true);
        TestChannelSftp channel = new TestChannelSftp(true, session);

        connectionPool.shutdown();
        connectionPool.disconnect(channel);

        Assertions.assertTrue(channel.wasDisconnected());
        Mockito.verify(session).disconnect();
        Assertions.assertEquals(0, connectionPool.getIdleCount());
        Assertions.assertEquals(0, connectionPool.getConnPoolSize());
    }

    /** Unknown channels are not reusable pool entries, so they must be closed immediately. */
    @Test
    void disconnectShouldCloseUntrackedChannel() throws Exception {
        SFTPConnectionPool connectionPool = new SFTPConnectionPool(1, 1);
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.isConnected()).thenReturn(true);
        TestChannelSftp channel = new TestChannelSftp(true, session);

        connectionPool.disconnect(channel);

        Assertions.assertTrue(channel.wasDisconnected());
        Mockito.verify(session).disconnect();
        Assertions.assertEquals(0, connectionPool.getIdleCount());
    }

    /** Small concrete ChannelSftp stub that keeps Mockito away from final JSch internals. */
    private static final class TestChannelSftp extends ChannelSftp {
        private final boolean connected;
        private final Session session;
        private boolean disconnected;

        private TestChannelSftp(boolean connected, Session session) {
            this.connected = connected;
            this.session = session;
        }

        @Override
        public boolean isConnected() {
            return connected;
        }

        @Override
        public void disconnect() {
            disconnected = true;
        }

        @Override
        public Session getSession() throws JSchException {
            return session;
        }

        private boolean wasDisconnected() {
            return disconnected;
        }
    }

    /** Read the private idle map to assert that the pool does not lose sibling channels. */
    @SuppressWarnings("unchecked")
    private static Map<SFTPConnectionPool.ConnectionInfo, HashSet<ChannelSftp>> idleConnections(
            SFTPConnectionPool connectionPool) throws Exception {
        Field field = SFTPConnectionPool.class.getDeclaredField("idleConnections");
        field.setAccessible(true);
        return (Map<SFTPConnectionPool.ConnectionInfo, HashSet<ChannelSftp>>)
                field.get(connectionPool);
    }

    /** Read the private tracked-channel map to seed deterministic pool state for unit tests. */
    @SuppressWarnings("unchecked")
    private static Map<ChannelSftp, SFTPConnectionPool.ConnectionInfo> trackedConnections(
            SFTPConnectionPool connectionPool) throws Exception {
        Field field = SFTPConnectionPool.class.getDeclaredField("con2infoMap");
        field.setAccessible(true);
        return (Map<ChannelSftp, SFTPConnectionPool.ConnectionInfo>) field.get(connectionPool);
    }
}
