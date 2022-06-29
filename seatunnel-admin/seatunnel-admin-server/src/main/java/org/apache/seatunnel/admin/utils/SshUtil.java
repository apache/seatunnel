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

package org.apache.seatunnel.admin.utils;

import net.schmizz.keepalive.KeepAliveProvider;
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.ConnectionException;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.FileSystemFile;
import net.schmizz.sshj.xfer.scp.SCPFileTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SshUtil implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SshUtil.class);

    private static final int DEFAULT_PORT = 22;

    private static final int DEFAULT_WAIT_TIME = 5;

    private String host;

    private int port;

    private String username;

    private String password;

    private SSHClient sshClient;

    private SFTPClient sftp;

    private SCPFileTransfer scpFileTransfer;

    private SshUtil(String host, int port, String username, String pasword) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = pasword;
        this.init();
    }

    public static SshUtil createInstance(String host, String username, String pasword) {
        return new SshUtil(host, DEFAULT_PORT, username, pasword);
    }

    public static SshUtil createInstance(String host, int port, String username, String pasword) {
        return new SshUtil(host, port, username, pasword);
    }

    private void init() {
        DefaultConfig defaultConfig = new DefaultConfig();
        defaultConfig.setKeepAliveProvider(KeepAliveProvider.KEEP_ALIVE);
        this.sshClient = new SSHClient(defaultConfig);
        this.sshClient.addHostKeyVerifier(new PromiscuousVerifier());
    }

    public boolean login() {
        if (sshClient.isAuthenticated() && sshClient.isConnected()) {
            return true;
        }
        try {
            sshClient.loadKnownHosts();
            if (!sshClient.isConnected()) {
                sshClient.connect(this.host, this.port);
                LOGGER.info("connect ======> {}", sshClient.isConnected() + "");
            }
            if (!sshClient.isAuthenticated()) {
                sshClient.authPassword(this.username, this.password);
                LOGGER.info("authPassword ======> {}", sshClient.isAuthenticated() + "");
            }
            return true;
        } catch (Exception ex) {
            LOGGER.warn(ex.getMessage(), ex);
            return false;
        }
    }

    public void download(String remotePath, String localPath) {
        if (!login()) {
            return;
        }
        try {
            if (scpFileTransfer == null) {
                scpFileTransfer = sshClient.newSCPFileTransfer();
            }
            scpFileTransfer.download(remotePath, new FileSystemFile(localPath));
        } catch (Exception ex) {
            LOGGER.warn(ex.getMessage(), ex);
        }
    }

    public void upload(String remotePath, String localPath) {
        if (!login()) {
            return;
        }
        try {
            if (scpFileTransfer == null) {
                scpFileTransfer = sshClient.newSCPFileTransfer();
            }
            scpFileTransfer.upload(new FileSystemFile(localPath), remotePath);
        } catch (Exception ex) {
            LOGGER.warn(ex.getMessage(), ex);
        }
    }

    public void get(String remotePath, String localPath) {
        if (!login()) {
            return;
        }
        try {
            if (sftp == null) {
                sftp = sshClient.newSFTPClient();
            }
            sftp.get(remotePath, new FileSystemFile(localPath));
        } catch (Exception ex) {
            LOGGER.warn(ex.getMessage(), ex);
        }
    }

    public void put(String remotePath, String localPath) {
        if (!login()) {
            return;
        }
        try {
            if (sftp == null) {
                sftp = sshClient.newSFTPClient();
            }
            sftp.put(new FileSystemFile(localPath), remotePath);
        } catch (Exception ex) {
            LOGGER.warn(ex.getMessage(), ex);
        }
    }

    /**
     * run shell command
     *
     * @param cmdContent shell command, "ping -c 1 www.baidu.com;cd /mnt;ls ./"
     */
    public String exec(String cmdContent) {
        if (!login()) {
            return null;
        }
        Session session = null;
        try {
            session = sshClient.startSession();
            final Session.Command cmd = session.exec(cmdContent);
            String resultLog = IOUtils.readFully(cmd.getInputStream()).toString();
            LOGGER.info(resultLog);
            cmd.join(DEFAULT_WAIT_TIME, TimeUnit.SECONDS);
            LOGGER.info("\n** exit status: " + cmd.getExitStatus());
            if (cmd.getExitStatus() == 0) {
                return resultLog;
            }
        } catch (Exception ex) {
            LOGGER.warn(ex.getMessage(), ex);
        }
        return null;
    }

    private void closeSession(Session session) throws TransportException, ConnectionException {
        if (session != null) {
            session.close();
        }
    }

    public void disconnect() throws IOException {
        if (sshClient != null) {
            sshClient.disconnect();
        }
    }

    @Override
    public void close() {
        try {
            if (sftp != null) {
                sftp.close();
                sftp = null;
            }
        } catch (IOException ignored) {
            LOGGER.error(ignored.getMessage(), ignored);
        }

        if (scpFileTransfer != null) {
            scpFileTransfer = null;
        }

        try {
            if (sshClient != null) {
                sshClient.disconnect();
                sshClient = null;
            }
        } catch (IOException ignored) {
            LOGGER.error(ignored.getMessage(), ignored);
        }
    }

}
