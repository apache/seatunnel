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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.ftp.config.FtpConf;
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
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

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

    /**
     * Wall-clock bound for the real-socket calls, deliberately unrelated to the timeout under test.
     *
     * <p>It only has to expire long after a working {@code control_timeout} would, so that a
     * regression surfaces as a bounded assertion failure instead of a hung build.
     */
    private static final long DEADLINE_SECONDS = 30;

    /** Handshake behaviour the fake client should exhibit, set before the call under test. */
    private static final class Server {
        int greetingCode = 220;
        /** Reply code the server reports for the login exchange: 230 accepted, 530 refused. */
        int loginReplyCode = 230;

        boolean loginAccepted = true;
        /**
         * When set, {@code connect()} throws this instead of succeeding, like a timed-out greeting.
         */
        IOException connectFailure;
        /** When set, {@code login()} throws this instead, like a timed-out login reply. */
        IOException loginFailure;

        void stub(FTPClient client) throws IOException {
            // connect() decides whether to continue by reading getReplyCode(), and login() runs
            // only after the greeting has been accepted. Stubbing one fixed reply code makes the
            // two exchanges indistinguishable: a refused-login code such as 530 would be rejected
            // as a bad greeting, so connect() would never reach login() and the refused-login
            // branch would never be exercised. The reply therefore follows the wire order: the
            // greeting code first, the login code once credentials have been sent.
            //
            // The flag is per client, not per server: the filesystem may construct more than one
            // client (getFileStatus resolves the home directory first), and a client that has not
            // sent credentials yet must still see a greeting.
            AtomicBoolean loginAttempted = new AtomicBoolean();
            when(client.getReplyCode())
                    .thenAnswer(reply -> loginAttempted.get() ? loginReplyCode : greetingCode);
            when(client.printWorkingDirectory()).thenReturn("/");
            when(client.isConnected()).thenReturn(true);
            if (connectFailure != null) {
                Mockito.doThrow(connectFailure).when(client).connect(anyString(), Mockito.anyInt());
                return;
            }
            if (loginFailure != null) {
                when(client.login(anyString(), anyString()))
                        .thenAnswer(
                                credentials -> {
                                    loginAttempted.set(true);
                                    throw loginFailure;
                                });
                return;
            }
            // A refused login reports the rejection through the reply code rather than by
            // throwing, so the code advances to loginReplyCode once credentials have been sent.
            when(client.login(anyString(), anyString()))
                    .thenAnswer(
                            credentials -> {
                                loginAttempted.set(true);
                                return loginAccepted;
                            });
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
        // The greeting must stay positive: a nonpositive greeting is rejected by connect() before
        // login() runs, so a 530 greeting would test the greeting branch twice and never reach
        // loginAccepted == false.
        server.greetingCode = 220;
        server.loginAccepted = false;
        server.loginReplyCode = 530;

        try (MockedConstruction<FTPClient> mocked = constructing(server)) {
            Assertions.assertThrows(
                    IOException.class, () -> fileSystem(baseConf()).getFileStatus(new Path("/")));

            // The socket is open by now, so leaving it to the garbage collector pins a control
            // connection on the server until the server gives up on it.
            FTPClient client = mocked.constructed().get(0);
            verify(client).connect(HOST, PORT);
            verify(client).login(anyString(), anyString());
            verify(client).disconnect();
            // QUIT must not be attempted on a server that just refused the login.
            verify(client, never()).logout();
        }
    }

    @Test
    void clientIsClosedWhenTheGreetingReadTimesOut() throws Exception {
        Server server = new Server();
        server.connectFailure = new SocketTimeoutException("Read timed out");

        try (MockedConstruction<FTPClient> mocked = constructing(server)) {
            Assertions.assertThrows(
                    SocketTimeoutException.class,
                    () -> fileSystem(baseConf()).getFileStatus(new Path("/")));

            FTPClient client = mocked.constructed().get(0);
            // connect() throws before the caller can receive the client, so this is the only
            // place that can release the socket the client allocated before reading the greeting.
            verify(client).disconnect();
            // logout() would write QUIT and wait for its reply on the channel that just timed
            // out, costing another full control_timeout before the failure surfaces.
            verify(client, never()).logout();
            verify(client, never()).login(anyString(), anyString());
        }
    }

    @Test
    void clientIsClosedWhenTheLoginReplyTimesOut() throws Exception {
        Server server = new Server();
        server.loginFailure = new SocketTimeoutException("Read timed out");

        try (MockedConstruction<FTPClient> mocked = constructing(server)) {
            Assertions.assertThrows(
                    SocketTimeoutException.class,
                    () -> fileSystem(baseConf()).getFileStatus(new Path("/")));

            FTPClient client = mocked.constructed().get(0);
            verify(client).disconnect();
            verify(client, never()).logout();
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
    void negativeControlTimeoutIsRejectedByBothFactories() {
        // Passed through unchecked, a negative value only fails later, inside Socket.setSoTimeout,
        // as an IllegalArgumentException that does not name the option the user set.
        //
        // Both configs are otherwise complete and valid. An incomplete config would also throw,
        // and the assertion below would then pass even with the control_timeout condition removed
        // -- it would merely be re-detecting a missing required option.
        Map<String, Object> source = new HashMap<>();
        source.put("host", HOST);
        source.put("port", PORT);
        source.put("user", "user");
        source.put("password", "password");
        source.put("path", "/tmp/seatunnel");
        source.put("control_timeout", -1);

        // The sink requires a path and a format; the source deliberately omits file_format_type so
        // that no format-conditional option (schema, delimiters) becomes required. Otherwise the
        // validation would report a second, unrelated error and the single-error assertion below
        // would no longer pin the control_timeout constraint.
        Map<String, Object> sink = new HashMap<>(source);
        sink.put("file_format_type", "text");

        assertOnlyControlTimeoutIsRejected(new FtpFileSourceFactory().optionRule(), source);
        assertOnlyControlTimeoutIsRejected(new FtpFileSinkFactory().optionRule(), sink);
    }

    /**
     * Asserts the rejection is the {@code control_timeout} value constraint and nothing else.
     *
     * <p>Asserting the exception type alone is not enough: any missing required option raises the
     * same type, so a config regression would look identical to the behaviour under test.
     */
    private static void assertOnlyControlTimeoutIsRejected(
            OptionRule rule, Map<String, Object> raw) {
        OptionValidationException failure =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(raw)).validate(rule));

        String message = failure.getRawMessage();
        Assertions.assertTrue(
                message.contains("(1 error)"),
                "Expected control_timeout to be the only rejected option, got:\n" + message);
        Assertions.assertTrue(
                message.contains("option: control_timeout"),
                "The failure should name control_timeout, got:\n" + message);
        Assertions.assertTrue(
                message.contains("type: value"),
                "The failure should be the value constraint, not a missing option, got:\n"
                        + message);
        Assertions.assertTrue(
                message.contains("'control_timeout' >= 0"),
                "The failure should quote the violated constraint, got:\n" + message);
    }

    @Test
    void controlTimeoutIsForwardedToTheHadoopConfiguration() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("host", HOST);
        raw.put("port", PORT);
        raw.put("user", "user");
        raw.put("password", "password");
        raw.put("control_timeout", 4_500);

        HadoopConf hadoopConf = FtpConf.buildWithConfig(ReadonlyConfig.fromMap(raw));

        // The value travels as a string through the Hadoop configuration map; a missing key would
        // silently fall back to the default whatever the user configured.
        Assertions.assertEquals(
                "4500",
                hadoopConf.getExtraOptions().get(SeaTunnelFTPFileSystem.FS_FTP_CONTROL_TIMEOUT));
    }

    /**
     * A real server that accepts the TCP connection and then stays silent, like a stalled peer.
     *
     * <p>The {@code FTPClient} is not intercepted here: the point is to exercise commons-net's own
     * read path so the socket read is genuinely bounded and the connection is genuinely released.
     * Anything that only asserts that {@code setDefaultTimeout} was called would pass even if the
     * value never reached the socket.
     */
    private static final class SilentFtpServer implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final Thread acceptor;
        private final CountDownLatch accepted = new CountDownLatch(1);
        private final CountDownLatch peerClosed = new CountDownLatch(1);
        /** Kept so close() can unblock a client that is still waiting on this connection. */
        private volatile Socket connection;

        SilentFtpServer() throws IOException {
            serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
            acceptor = new Thread(this::serve, "silent-ftp-server");
            acceptor.setDaemon(true);
        }

        void start() {
            acceptor.start();
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        void awaitAccepted() throws InterruptedException {
            Assertions.assertTrue(
                    accepted.await(10, TimeUnit.SECONDS),
                    "The client never opened the control connection");
        }

        /**
         * Waits for the client to close its side. A read returning -1 or throwing means the peer
         * sent FIN or RST; a socket left to the garbage collector would keep this waiting.
         */
        void awaitPeerClosed() throws InterruptedException {
            Assertions.assertTrue(
                    peerClosed.await(10, TimeUnit.SECONDS),
                    "The control connection was not closed after the handshake failed");
        }

        private void serve() {
            try (Socket socket = serverSocket.accept()) {
                connection = socket;
                accepted.countDown();
                // Never write the 220 greeting. Read forever so a FIN from the client is observed
                // as end-of-stream here.
                while (socket.getInputStream().read() != -1) {
                    // discard anything the client sends; this server never replies
                }
            } catch (IOException e) {
                // A reset from the client is also an observable close.
            } finally {
                peerClosed.countDown();
            }
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
            // Releasing the accepted socket lets a client still blocked in a read (the
            // control_timeout = 0 case) finish instead of leaking its thread past the test.
            Socket open = connection;
            if (open != null) {
                open.close();
            }
        }
    }

    @Test
    void stalledGreetingFailsWithinTheConfiguredTimeoutAndClosesTheConnection() throws Exception {
        try (SilentFtpServer server = new SilentFtpServer()) {
            server.start();
            Configuration conf = confFor(server.port());
            // Short enough to keep the test fast, long enough not to race a slow CI machine.
            conf.setInt(SeaTunnelFTPFileSystem.FS_FTP_CONTROL_TIMEOUT, 1_000);

            Throwable failure =
                    callWithinDeadline(
                            server,
                            () -> {
                                fileSystem(conf, server.port()).getFileStatus(new Path("/"));
                                return null;
                            });

            server.awaitAccepted();
            Assertions.assertInstanceOf(
                    SocketTimeoutException.class,
                    failure,
                    () ->
                            "The stalled greeting should fail with a socket timeout, got: "
                                    + failure);
            Assertions.assertNotNull(failure.getMessage());
            // Direct close, not logout: the server never replies, so QUIT would block again and
            // the connection would still be open when the exception surfaces.
            server.awaitPeerClosed();
        }
    }

    @Test
    void zeroControlTimeoutDoesNotBoundTheStalledGreeting() throws Exception {
        SilentFtpServer server = new SilentFtpServer();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        boolean terminated;
        try {
            server.start();
            Configuration conf = confFor(server.port());
            // 0 restores the previous wait-indefinitely behaviour, so the call must still be
            // blocked well past the timeout the other test proves is effective.
            conf.setInt(SeaTunnelFTPFileSystem.FS_FTP_CONTROL_TIMEOUT, 0);

            Future<?> call =
                    executor.submit(
                            () -> fileSystem(conf, server.port()).getFileStatus(new Path("/")));
            server.awaitAccepted();
            // An explicit bounded observation rather than a sleep-then-isDone: a call that is
            // still blocked is the positive result here, so the timeout is the assertion.
            Assertions.assertThrows(
                    TimeoutException.class,
                    () -> call.get(1_500, TimeUnit.MILLISECONDS),
                    "control_timeout = 0 must restore the unbounded wait, but the call returned");
        } finally {
            // Closing the peer is what releases a read that no socket timeout will release, so it
            // has to happen before the executor is checked for termination; awaiting first would
            // always time out and report a leak that is really this ordering.
            server.close();
            executor.shutdownNow();
            terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        Assertions.assertTrue(
                terminated,
                "The blocked control read outlived the test even after the peer was closed");
    }

    /**
     * Runs the call on its own thread under a deadline that is independent of {@code
     * control_timeout}.
     *
     * <p>The regression this test exists to catch is a control read that is no longer bounded.
     * Invoking the filesystem inline would then never return, so neither the assertion nor the
     * cleanup would run and the build would hang instead of reporting a failure. Returning the
     * failure instead of asserting it keeps every phase of the test interruptible.
     *
     * <p>The peer is force-closed only when the call is still blocked, which can only happen on the
     * regression path: an ordinary failure has already released its socket, so closing here
     * unconditionally would make the caller's peer-closed check vacuous.
     */
    private static Throwable callWithinDeadline(SilentFtpServer server, Callable<Void> call)
            throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Void> pending = executor.submit(call);
        Throwable outcome;
        boolean terminated;
        try {
            pending.get(DEADLINE_SECONDS, TimeUnit.SECONDS);
            outcome =
                    new AssertionError(
                            "The stalled greeting did not fail the call, so the control read is"
                                    + " apparently not bounded at all");
        } catch (ExecutionException e) {
            outcome = e.getCause();
        } catch (TimeoutException e) {
            outcome =
                    new AssertionError(
                            "The call was still blocked after "
                                    + DEADLINE_SECONDS
                                    + " s although control_timeout was 1000 ms; a job would stay"
                                    + " RUNNING on a silent server",
                            e);
        } finally {
            if (!pending.isDone()) {
                // Only reachable after a regression: closing the peer is the sole thing that
                // releases a read no socket timeout is bounding.
                server.close();
            }
            executor.shutdownNow();
            terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        Assertions.assertTrue(
                terminated, "The control connection was not released, so its thread leaked");
        return outcome;
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

    /** Configuration pointing at a real local server instead of the mocked {@link #HOST}. */
    private static Configuration confFor(int port) {
        Configuration conf = baseConf();
        conf.set(SeaTunnelFTPFileSystem.FS_FTP_HOST, "localhost");
        conf.setInt(SeaTunnelFTPFileSystem.FS_FTP_HOST_PORT, port);
        conf.set(SeaTunnelFTPFileSystem.FS_FTP_USER_PREFIX + "localhost", "user");
        conf.set(SeaTunnelFTPFileSystem.FS_FTP_PASSWORD_PREFIX + "localhost", "password");
        return conf;
    }

    private static SeaTunnelFTPFileSystem fileSystem(Configuration conf, int port)
            throws IOException, URISyntaxException {
        return fileSystem(conf, new URI("ftp://localhost:" + port));
    }

    private static SeaTunnelFTPFileSystem fileSystem(Configuration conf, URI uri)
            throws IOException {
        SeaTunnelFTPFileSystem fs = new SeaTunnelFTPFileSystem();
        fs.initialize(uri, conf);
        return fs;
    }

    private static SeaTunnelFTPFileSystem fileSystem(Configuration conf)
            throws IOException, URISyntaxException {
        return fileSystem(conf, new URI("ftp://" + HOST + ":" + PORT));
    }
}
