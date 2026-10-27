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

package org.apache.seatunnel.connectors.seatunnel.file.ftp.system;

import org.apache.seatunnel.connectors.seatunnel.file.ftp.config.FtpFileBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.file.ftp.sink.FtpFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.ftp.source.FtpFileSourceFactory;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers the control-connection read timeout.
 *
 * <p>The timeout cannot be observed from outside the client: {@code FTPClient} exposes no getter
 * for the value actually in effect on an accepted socket, and a timeout set after the greeting has
 * been read is invisible. The test therefore intercepts construction and asserts the exact value
 * handed to the client, plus the order relative to {@code connect()}, which is what decides whether
 * the greeting read is protected.
 */
class SeaTunnelFTPFileSystemTimeoutTest {

    private static final String HOST = "ftp.example.com";
    private static final int PORT = 21;
    private static final int DEFAULT_CONTROL_TIMEOUT = 60_000;

    /** Handshake behaviour the fake client should exhibit, set before the call under test. */
    private static final class Server {
        int greetingCode = 220;
        boolean loginAccepted = true;

        void stub(FTPClient client) throws IOException {
            when(client.getReplyCode()).thenReturn(greetingCode);
            when(client.login(anyString(), anyString())).thenReturn(loginAccepted);
            when(client.printWorkingDirectory()).thenReturn("/");
            when(client.isConnected()).thenReturn(true);
        }
    }

    @Test
    void controlTimeoutDefaultsToSixtySeconds() throws Exception {
        assertTimeoutApplied(DEFAULT_CONTROL_TIMEOUT, baseConf());
    }

    @Test
    void controlTimeoutUsesTheConfiguredValue() throws Exception {
        Configuration conf = baseConf();
        conf.setInt(SeaTunnelFTPFileSystem.FS_FTP_CONTROL_TIMEOUT, 2_000);
        assertTimeoutApplied(2_000, conf);
    }

    @Test
    void zeroRestoresTheWaitIndefinitelyBehaviour() throws Exception {
        Configuration conf = baseConf();
        conf.setInt(SeaTunnelFTPFileSystem.FS_FTP_CONTROL_TIMEOUT, 0);
        assertTimeoutApplied(0, conf);
    }

    private void assertTimeoutApplied(int expected, Configuration conf) throws Exception {
        Server server = new Server();
        try (MockedConstruction<FTPClient> mocked = constructing(server)) {
            fileSystem(conf).getFileStatus(new Path("/"));

            verify(mocked.constructed().get(0)).setDefaultTimeout(expected);
        }
    }

    @Test
    void controlTimeoutIsAppliedBeforeConnecting() throws Exception {
        Configuration conf = baseConf();
        conf.setInt(SeaTunnelFTPFileSystem.FS_FTP_CONTROL_TIMEOUT, 1_500);

        try (MockedConstruction<FTPClient> mocked = constructing(new Server())) {
            fileSystem(conf).getFileStatus(new Path("/"));
            FTPClient client = mocked.constructed().get(0);

            // Applying the timeout after connect() leaves the greeting unprotected, and that read
            // is exactly the one that hangs when a server accepts the connection and stays silent.
            InOrder order = Mockito.inOrder(client);
            order.verify(client).setDefaultTimeout(1_500);
            order.verify(client).connect(HOST, PORT);
        }
    }

    @Test
    void clientIsDisconnectedWhenTheServerDoesNotAcceptTheLogin() throws Exception {
        Server server = new Server();
        server.loginAccepted = false;
        server.greetingCode = 530;

        try (MockedConstruction<FTPClient> mocked = constructing(server)) {
            Assertions.assertThrows(
                    IOException.class, () -> fileSystem(baseConf()).getFileStatus(new Path("/")));

            // The socket is open by now, so leaving it to the garbage collector pins a control
            // connection on the server until the server gives up on it.
            verify(mocked.constructed().get(0)).disconnect();
        }
    }

    @Test
    void clientIsDisconnectedWhenTheGreetingIsRejected() throws Exception {
        Server server = new Server();
        server.greetingCode = 421;

        try (MockedConstruction<FTPClient> mocked = constructing(server)) {
            Assertions.assertThrows(
                    IOException.class, () -> fileSystem(baseConf()).getFileStatus(new Path("/")));

            FTPClient client = mocked.constructed().get(0);
            verify(client).connect(HOST, PORT);
            verify(client).disconnect();
            verify(client, never()).login(anyString(), anyString());
        }
    }

    @Test
    void successfulCallClosesTheConnectionExactlyOnce() throws Exception {
        try (MockedConstruction<FTPClient> mocked = constructing(new Server())) {
            fileSystem(baseConf()).getFileStatus(new Path("/"));

            // The finally block in the caller owns the release. The new failure-path disconnect
            // must not add a second one, otherwise a successful call would tear the client down
            // twice and the second attempt would throw on an already closed socket.
            verify(mocked.constructed().get(0), Mockito.times(1)).disconnect();
        }
    }

    @Test
    void controlTimeoutOptionIsDeclaredForBothSourceAndSink() {
        // The control channel is shared by reads and writes, so a user who can only configure one
        // side would still get the unbounded wait on the other.
        Assertions.assertTrue(
                new FtpFileSourceFactory()
                        .optionRule()
                        .getOptionalOptions()
                        .contains(FtpFileBaseOptions.FTP_CONTROL_TIMEOUT));
        Assertions.assertTrue(
                new FtpFileSinkFactory()
                        .optionRule()
                        .getOptionalOptions()
                        .contains(FtpFileBaseOptions.FTP_CONTROL_TIMEOUT));
    }

    @Test
    void controlTimeoutIsForwardedToTheHadoopConfiguration() {
        java.util.Map<String, Object> raw = new java.util.HashMap<>();
        raw.put("host", HOST);
        raw.put("port", PORT);
        raw.put("user", "user");
        raw.put("password", "password");
        raw.put("control_timeout", 4_500);

        org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf hadoopConf =
                org.apache.seatunnel.connectors.seatunnel.file.ftp.config.FtpConf.buildWithConfig(
                        org.apache.seatunnel.api.configuration.ReadonlyConfig.fromMap(raw));

        // The value travels as a string through the Hadoop configuration map; a missing key would
        // silently fall back to the default whatever the user configured.
        Assertions.assertEquals(
                "4500",
                hadoopConf.getExtraOptions().get(SeaTunnelFTPFileSystem.FS_FTP_CONTROL_TIMEOUT));
    }

    private static MockedConstruction<FTPClient> constructing(Server server) {
        return mockConstruction(
                FTPClient.class,
                (mock, context) -> {
                    try {
                        server.stub(mock);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                });
    }

    private static Configuration baseConf() {
        Configuration conf = new Configuration();
        conf.set(SeaTunnelFTPFileSystem.FS_FTP_HOST, HOST);
        conf.setInt(SeaTunnelFTPFileSystem.FS_FTP_HOST_PORT, PORT);
        conf.set(SeaTunnelFTPFileSystem.FS_FTP_USER_PREFIX + HOST, "user");
        conf.set(SeaTunnelFTPFileSystem.FS_FTP_PASSWORD_PREFIX + HOST, "password");
        conf.set(SeaTunnelFTPFileSystem.FS_FTP_CONNECTION_MODE, "passive_local");
        return conf;
    }

    private static SeaTunnelFTPFileSystem fileSystem(Configuration conf)
            throws IOException, java.net.URISyntaxException {
        SeaTunnelFTPFileSystem fs = new SeaTunnelFTPFileSystem();
        fs.initialize(new URI("ftp://" + HOST + ":" + PORT), conf);
        return fs;
    }
}
