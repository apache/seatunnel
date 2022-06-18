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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.scp.client.ScpClient;
import org.apache.sshd.scp.client.ScpClientCreator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ScpFileTransfer implements FileTransfer {

    private static final int SCP_PORT = 22;

    private final String host;
    private final String password;

    private ScpClient scpClient;
    private ClientSession clientSession;
    private SshClient sshClient;

    public ScpFileTransfer(String host, String password) {
        this.host = host;
        this.password = password;
    }

    @Override
    public void init() {
        try {
            sshClient = SshClient.setUpDefaultClient();
            sshClient.start();
            clientSession = sshClient.connect("root", host, SCP_PORT).verify().getSession();
            if (password != null) {
                clientSession.addPasswordIdentity(password);
            }
            if (!clientSession.auth().verify().isSuccess()) {
                throw new IOException("ssh host " + host + "authentication failed");
            }
            scpClient = ScpClientCreator.instance().createScpClient(clientSession);
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to host: " + host + " by user: root on port 22", e);
        }
    }

    @Override
    public void transferAndChown(String sourcePath, String targetPath) {
        try {
            scpClient.upload(
                sourcePath,
                targetPath,
                ScpClient.Option.Recursive,
                ScpClient.Option.TargetIsDirectory,
                ScpClient.Option.PreserveAttributes);
        } catch (IOException e) {
            throw new RuntimeException("Scp failed to transfer file: " + sourcePath + " to: " + targetPath, e);
        }
        // remote exec command to change file owner. Only file owner equal with server's clickhouse user can
        // make ATTACH command work.
        List<String> command = new ArrayList<>();
        command.add("ls");
        command.add("-l");
        command.add(targetPath.substring(0, targetPath.lastIndexOf("/")));
        command.add("/ | tail -n 1 | awk '{print $3}' | xargs -t -i chown -R {}:{} " + targetPath);
        try {
            clientSession.executeRemoteCommand(String.join(" ", command));
        } catch (IOException e) {
            throw new RuntimeException("Failed to execute remote command: " + command, e);
        }
    }

    @Override
    public void transferAndChown(List<String> sourcePaths, String targetPath) {
        if (sourcePaths == null) {
            throw new IllegalArgumentException("sourcePath is null");
        }
        sourcePaths.forEach(sourcePath -> transferAndChown(sourcePath, targetPath));
    }

    @Override
    public void close() {
        if (clientSession != null && clientSession.isOpen()) {
            try {
                clientSession.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close ssh session", e);
            }
        }
        if (sshClient != null && sshClient.isOpen()) {
            sshClient.stop();
            try {
                sshClient.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close ssh client", e);
            }
        }
    }
}
