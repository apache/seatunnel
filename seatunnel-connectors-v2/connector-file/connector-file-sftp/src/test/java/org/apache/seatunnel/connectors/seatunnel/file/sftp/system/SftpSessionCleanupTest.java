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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;

class SftpSessionCleanupTest {

    /**
     * Continuous SFTP jobs can cancel after the channel is already marked disconnected, but the
     * underlying JSch session reader thread must still be closed to avoid hanging container
     * shutdown.
     */
    @Test
    void shouldDisconnectSessionWhenChannelAlreadyClosedInPool() throws Exception {
        SFTPConnectionPool pool = new SFTPConnectionPool(1, 1);
        RecordingChannelSftp channel = new RecordingChannelSftp(false);
        Session session = Mockito.mock(Session.class);
        channel.setSession(session);
        Mockito.when(session.isConnected()).thenReturn(true);

        pool.disconnect(channel);

        org.junit.jupiter.api.Assertions.assertFalse(channel.isChannelDisconnectCalled());
        Mockito.verify(session).disconnect();
    }

    @Test
    void shouldKeepOtherIdleConnectionsWhenBorrowingFromPool() throws Exception {
        SFTPConnectionPool pool = new SFTPConnectionPool(2, 2);
        SFTPConnectionPool.ConnectionInfo info =
                new SFTPConnectionPool.ConnectionInfo("localhost", 22, "seatunnel");
        RecordingChannelSftp first = new RecordingChannelSftp(true);
        RecordingChannelSftp second = new RecordingChannelSftp(true);
        HashSet<ChannelSftp> idleChannels = new HashSet<>();
        idleChannels.add(first);
        idleChannels.add(second);
        HashMap<SFTPConnectionPool.ConnectionInfo, HashSet<ChannelSftp>> idleConnections =
                new HashMap<>();
        idleConnections.put(info, idleChannels);
        setField(pool, "idleConnections", idleConnections);

        ChannelSftp borrowedOne = pool.getFromPool(info);
        ChannelSftp borrowedTwo = pool.getFromPool(info);

        org.junit.jupiter.api.Assertions.assertNotNull(borrowedOne);
        org.junit.jupiter.api.Assertions.assertNotNull(borrowedTwo);
        org.junit.jupiter.api.Assertions.assertNotSame(borrowedOne, borrowedTwo);
    }

    @Test
    void shouldDisconnectTrackedSessionDuringShutdownWhenLiveCountDrifts() throws Exception {
        SFTPConnectionPool pool = new SFTPConnectionPool(1, 0);
        RecordingChannelSftp channel = new RecordingChannelSftp(false);
        Session session = Mockito.mock(Session.class);
        channel.setSession(session);
        Mockito.when(session.isConnected()).thenReturn(true);

        HashMap<ChannelSftp, SFTPConnectionPool.ConnectionInfo> trackedConnections =
                new HashMap<>();
        trackedConnections.put(
                channel, new SFTPConnectionPool.ConnectionInfo("localhost", 22, "seatunnel"));
        setField(pool, "con2infoMap", trackedConnections);

        pool.shutdown();

        Mockito.verify(session).disconnect();
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    /**
     * Stream close should release the JSch session even when the channel transitioned to
     * disconnected before the wrapper stream finished cleaning up.
     */
    @Test
    void shouldDisconnectSessionWhenChannelAlreadyClosedInInputStream() throws Exception {
        RecordingChannelSftp channel = new RecordingChannelSftp(true);
        Session session = Mockito.mock(Session.class);
        channel.setSession(session);
        Mockito.when(session.isConnected()).thenReturn(true);

        SFTPInputStream inputStream =
                new SFTPInputStream(new ByteArrayInputStream(new byte[0]), channel, null);
        channel.setConnected(false);
        inputStream.close();

        org.junit.jupiter.api.Assertions.assertFalse(channel.isChannelDisconnectCalled());
        Mockito.verify(session).disconnect();
    }

    private static final class RecordingChannelSftp extends ChannelSftp {

        private boolean connected;
        private boolean channelDisconnectCalled;
        private Session session;

        private RecordingChannelSftp(boolean connected) {
            this.connected = connected;
        }

        private void setConnected(boolean connected) {
            this.connected = connected;
        }

        private void setSession(Session session) {
            this.session = session;
        }

        private boolean isChannelDisconnectCalled() {
            return channelDisconnectCalled;
        }

        @Override
        public boolean isConnected() {
            return connected;
        }

        @Override
        public Session getSession() {
            return session;
        }

        @Override
        public void disconnect() {
            channelDisconnectCalled = true;
            connected = false;
        }
    }
}
