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

class SftpSessionCleanupTest {

    /**
     * Continuous SFTP jobs can cancel after the channel is already marked disconnected, but the
     * underlying JSch session reader thread must still be closed to avoid hanging container
     * shutdown.
     */
    @Test
    void shouldDisconnectSessionWhenChannelAlreadyClosedInPool() throws Exception {
        SFTPConnectionPool pool = new SFTPConnectionPool(0, 1);
        RecordingChannelSftp channel = new RecordingChannelSftp(false);
        Session session = Mockito.mock(Session.class);
        channel.setSession(session);
        Mockito.when(session.isConnected()).thenReturn(true);

        pool.disconnect(channel);

        org.junit.jupiter.api.Assertions.assertFalse(channel.isChannelDisconnectCalled());
        Mockito.verify(session).disconnect();
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
