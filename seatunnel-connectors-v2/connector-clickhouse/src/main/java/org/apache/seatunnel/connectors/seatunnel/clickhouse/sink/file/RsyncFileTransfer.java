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

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.session.ClientSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RsyncFileTransfer implements FileTransfer {

    private static final int SSH_PORT = 22;

    private final String host;
    private final String user;
    private final String password;

    private ClientSession clientSession;
    private SshClient sshClient;

    public RsyncFileTransfer(String host, String user, String password) {
        this.host = host;
        this.user = user;
        this.password = password;
    }

    @Override
    public void init() {
        try {
            sshClient = SshClient.setUpDefaultClient();
            sshClient.start();
            clientSession = sshClient.connect(user, host, SSH_PORT).verify().getSession();
            if (password != null) {
                clientSession.addPasswordIdentity(password);
            }
            // TODO support add publicKey to identity
            if (!clientSession.auth().verify().isSuccess()) {
                throw new ClickhouseConnectorException(ClickhouseConnectorErrorCode.SSH_OPERATION_FAILED, "ssh host " + host + "authentication failed");
            }
        } catch (IOException e) {
            throw new ClickhouseConnectorException(ClickhouseConnectorErrorCode.SSH_OPERATION_FAILED,
                "Failed to connect to host: " + host + " by user: " + user + " on port 22", e);
        }
    }

    @Override
    public void transferAndChown(String sourcePath, String targetPath) {
        try {
            String sshParameter = password != null ? String.format("'sshpass -p %s ssh -o StrictHostKeyChecking=no -p %s'", password, SSH_PORT) : String.format("'ssh -o StrictHostKeyChecking=no -p %s'", SSH_PORT);
            List<String> rsyncCommand = new ArrayList<>();
            rsyncCommand.add("rsync");
            // recursive with -r
            rsyncCommand.add("-r");
            // compress during transfer file with -z
            rsyncCommand.add("-z");
            // output detail log with -v
            rsyncCommand.add("-v");
            // use ssh protocol with -e
            rsyncCommand.add("-e");
            rsyncCommand.add(sshParameter);
            rsyncCommand.add(sourcePath);
            rsyncCommand.add(String.format("root@%s:%s", host, targetPath));
            log.info("Generate rsync command: {}", String.join(" ", rsyncCommand));
            ProcessBuilder processBuilder = new ProcessBuilder("bash", "-c", String.join(" ", rsyncCommand));
            Process start = processBuilder.start();
            // we just wait for the process to finish
            try (InputStream inputStream = start.getInputStream();
                 InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                 BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    log.info(line);
                }
            }
            start.waitFor();
        } catch (IOException | InterruptedException ex) {
            throw new ClickhouseConnectorException(CommonErrorCode.FILE_OPERATION_FAILED, "Rsync failed to transfer file: " + sourcePath + " to: " + targetPath, ex);
        }
        // remote exec command to change file owner. Only file owner equal with server's clickhouse user can
        // make ATTACH command work.
        List<String> command = new ArrayList<>();
        command.add("ls");
        command.add("-l");
        command.add(targetPath.substring(0,
            StringUtils.stripEnd(targetPath, "/").lastIndexOf("/")) + "/");
        command.add("| tail -n 1 | awk '{print $3}' | xargs -t -i chown -R {}:{} " + targetPath);
        try {
            String finalCommand = String.join(" ", command);
            log.info("execute remote command: " + finalCommand);
            clientSession.executeRemoteCommand(finalCommand);
        } catch (IOException e) {
            // always return error cause xargs return shell command result
        }
    }

    @Override
    public void transferAndChown(List<String> sourcePaths, String targetPath) {
        if (sourcePaths == null) {
            throw new ClickhouseConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT, "sourcePath is null");
        }
        sourcePaths.forEach(sourcePath -> transferAndChown(sourcePath, targetPath));
    }

    @Override
    public void close() {
        if (clientSession != null && clientSession.isOpen()) {
            try {
                clientSession.close();
            } catch (IOException e) {
                throw new ClickhouseConnectorException(ClickhouseConnectorErrorCode.SSH_OPERATION_FAILED, "Failed to close ssh session", e);
            }
        }
        if (sshClient != null && sshClient.isOpen()) {
            sshClient.stop();
            try {
                sshClient.close();
            } catch (IOException e) {
                throw new ClickhouseConnectorException(ClickhouseConnectorErrorCode.SSH_OPERATION_FAILED, "Failed to close ssh client", e);
            }
        }
    }
}
