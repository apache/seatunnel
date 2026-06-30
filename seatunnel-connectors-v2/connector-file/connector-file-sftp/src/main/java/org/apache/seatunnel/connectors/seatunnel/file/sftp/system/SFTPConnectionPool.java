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

import org.apache.hadoop.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class SFTPConnectionPool {

    public static final Logger LOG = LoggerFactory.getLogger(SFTPFileSystem.class);
    // Maximum number of allowed live connections. This doesn't mean we cannot
    // have more live connections. It means that when we have more
    // live connections than this threshold, any unused connection will be
    // closed.
    private int maxConnection;
    private int liveConnectionCount;
    private HashMap<ConnectionInfo, HashSet<ChannelSftp>> idleConnections =
            new HashMap<ConnectionInfo, HashSet<ChannelSftp>>();
    private HashMap<ChannelSftp, ConnectionInfo> con2infoMap =
            new HashMap<ChannelSftp, ConnectionInfo>();

    SFTPConnectionPool(int maxConnection, int liveConnectionCount) {
        this.maxConnection = maxConnection;
        this.liveConnectionCount = liveConnectionCount;
    }

    synchronized ChannelSftp getFromPool(ConnectionInfo info) throws IOException {
        if (con2infoMap == null) {
            throw new IOException("SFTP connection pool has been closed.");
        }
        Set<ChannelSftp> cons = idleConnections.get(info);
        ChannelSftp channel;

        if (cons != null && cons.size() > 0) {
            Iterator<ChannelSftp> it = cons.iterator();
            if (it.hasNext()) {
                channel = it.next();
                it.remove();
                if (cons.isEmpty()) {
                    idleConnections.remove(info);
                }
                return channel;
            } else {
                throw new IOException("Connection pool error.");
            }
        }
        return null;
    }

    synchronized boolean returnToPool(ChannelSftp channel) {
        if (con2infoMap == null) {
            return false;
        }
        ConnectionInfo info = con2infoMap.get(channel);
        if (info == null) {
            return false;
        }
        HashSet<ChannelSftp> cons = idleConnections.get(info);
        if (cons == null) {
            cons = new HashSet<ChannelSftp>();
            idleConnections.put(info, cons);
        }
        cons.add(channel);
        return true;
    }

    /** Shutdown the connection pool and close all open connections. */
    void shutdown() {
        Map<ChannelSftp, ConnectionInfo> connectionsToClose;
        synchronized (this) {
            if (this.con2infoMap == null) {
                return; // already shutdown in case it is called
            }
            LOG.info("Inside shutdown, con2infoMap size=" + con2infoMap.size());

            // Shutdown must close every tracked connection regardless of live-count drift.
            connectionsToClose = new HashMap<ChannelSftp, ConnectionInfo>(con2infoMap);
            this.maxConnection = 0;
            this.liveConnectionCount = 0;
            this.idleConnections = null;
            this.con2infoMap = null;
        }

        for (Map.Entry<ChannelSftp, ConnectionInfo> entry : connectionsToClose.entrySet()) {
            try {
                closeChannel(entry.getKey());
            } catch (IOException ioe) {
                LOG.error(
                        "Error encountered while closing connection to "
                                + entry.getValue().getHost(),
                        ioe);
            }
        }
    }

    public synchronized int getMaxConnection() {
        return maxConnection;
    }

    public synchronized void setMaxConnection(int maxConn) {
        this.maxConnection = maxConn;
    }

    public ChannelSftp connect(String host, int port, String user, String password, String keyFile)
            throws IOException {
        // get connection from pool
        ConnectionInfo info = new ConnectionInfo(host, port, user);
        ChannelSftp channel = getFromPool(info);

        if (channel != null) {
            if (channel.isConnected()) {
                return channel;
            } else {
                removeTrackedChannel(channel);
                closeChannel(channel);
                channel = null;
            }
        }

        // create a new connection and add to pool
        JSch jsch = new JSch();
        Session session = null;
        try {
            if (user == null || user.length() == 0) {
                user = System.getProperty("user.name");
            }

            if (password == null) {
                password = "";
            }

            if (keyFile != null && keyFile.length() > 0) {
                jsch.addIdentity(keyFile);
            }

            if (port <= 0) {
                session = jsch.getSession(user, host);
            } else {
                session = jsch.getSession(user, host, port);
            }

            // JSch creates a session reader thread; make it daemon so leaked sessions cannot keep
            // Spark local-mode JVMs alive after a batch job has already finished.
            session.setDaemonThread(true);
            session.setPassword(password);

            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            session.connect();
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();

            synchronized (this) {
                con2infoMap.put(channel, info);
                liveConnectionCount++;
            }

            return channel;

        } catch (JSchException e) {
            throw new IOException(StringUtils.stringifyException(e));
        }
    }

    void disconnect(ChannelSftp channel) throws IOException {
        if (channel != null) {
            // close connection if too many active connections
            boolean closeConnection = false;
            synchronized (this) {
                if (con2infoMap == null || !con2infoMap.containsKey(channel)) {
                    closeConnection = true;
                } else if (liveConnectionCount > maxConnection) {
                    --liveConnectionCount;
                    con2infoMap.remove(channel);
                    closeConnection = true;
                }
            }
            if (closeConnection) {
                closeChannel(channel);
            } else if (!returnToPool(channel)) {
                closeChannel(channel);
            }
        }
    }

    /**
     * Remove bookkeeping for a channel that can no longer be reused before we create a replacement
     * connection.
     */
    synchronized void removeTrackedChannel(ChannelSftp channel) {
        if (con2infoMap == null) {
            return;
        }
        ConnectionInfo info = con2infoMap.remove(channel);
        if (info != null) {
            Set<ChannelSftp> cons = idleConnections.get(info);
            if (cons != null) {
                cons.remove(channel);
                if (cons.isEmpty()) {
                    idleConnections.remove(info);
                }
            }
        }
        if (liveConnectionCount > 0) {
            liveConnectionCount--;
        }
    }

    /** Close both the SFTP channel and its backing SSH session when they are still open. */
    private void closeChannel(ChannelSftp channel) throws IOException {
        try {
            Session session = channel.getSession();
            if (channel.isConnected()) {
                channel.disconnect();
            }
            if (session != null && session.isConnected()) {
                session.disconnect();
            }
        } catch (JSchException e) {
            throw new IOException(StringUtils.stringifyException(e));
        }
    }

    public int getIdleCount() {
        return this.idleConnections == null ? 0 : this.idleConnections.size();
    }

    public int getLiveConnCount() {
        return this.liveConnectionCount;
    }

    public int getConnPoolSize() {
        return this.con2infoMap == null ? 0 : this.con2infoMap.size();
    }

    /**
     * Class to capture the minimal set of information that distinguish between different
     * connections.
     */
    static class ConnectionInfo {
        private String host = "";
        private int port;
        private String user = "";

        ConnectionInfo(String hst, int prt, String usr) {
            this.host = hst;
            this.port = prt;
            this.user = usr;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String hst) {
            this.host = hst;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int prt) {
            this.port = prt;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String usr) {
            this.user = usr;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj instanceof ConnectionInfo) {
                ConnectionInfo con = (ConnectionInfo) obj;

                boolean ret = true;
                if (this.host == null || !this.host.equalsIgnoreCase(con.host)) {
                    ret = false;
                }
                if (this.port >= 0 && this.port != con.port) {
                    ret = false;
                }
                if (this.user == null || !this.user.equalsIgnoreCase(con.user)) {
                    ret = false;
                }
                return ret;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            if (host != null) {
                hashCode += host.hashCode();
            }
            hashCode += port;
            if (user != null) {
                hashCode += user.hashCode();
            }
            return hashCode;
        }
    }
}
