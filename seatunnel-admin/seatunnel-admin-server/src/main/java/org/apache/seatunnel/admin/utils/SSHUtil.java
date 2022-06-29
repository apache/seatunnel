/*
 * Copyright 2002-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.FileSystemFile;
import net.schmizz.sshj.xfer.scp.SCPFileTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * SSH 工具类
 * <p>
 *     参考地址： https://github.com/hierynomus/sshj/tree/master/examples
 * </p>
 * @author zhian
 * @version 1.0
 * @since 2021/6/28 14:04
 **/
public class SSHUtil implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SSHUtil.class);

    /**
     * 远程服务器地址
     */
    private String host;

    /**
     * 远程服务器SSH端口号
     */
    private int port;

    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * SSH客户端实例
     */
    private SSHClient sshClient;

    private SFTPClient sftp;

    private SCPFileTransfer scpFileTransfer;

    /**
     * 构造函数
     *
     * @param host
     * @param port
     * @param username
     * @param pasword
     */
    private SSHUtil(String host, int port, String username, String pasword) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = pasword;
        this.init();
    }

    /**
     * 创建实例
     * @param host 服务器地址
     * @param username 登录用户
     * @param pasword 登录密码
     * @return
     */
    public static SSHUtil createInstance(String host, String username, String pasword) {
        return new SSHUtil(host, 22, username, pasword);
    }

    /**
     * 创建实例
     * @param host 服务器地址
     * @param port 服务器端口
     * @param username 登录用户
     * @param pasword 登录密码
     * @return
     */
    public static SSHUtil createInstance(String host, int port, String username, String pasword) {
        return new SSHUtil(host, port, username, pasword);
    }

    private void init() {
        DefaultConfig defaultConfig = new DefaultConfig();
        defaultConfig.setKeepAliveProvider(KeepAliveProvider.KEEP_ALIVE);
        this.sshClient = new SSHClient(defaultConfig);
        // 如果需要使用用户和密码方式进行连接操作,此代码必须执行
        this.sshClient.addHostKeyVerifier(new PromiscuousVerifier());
    }

    /**
     * 登录
     *
     * @return
     */
    public boolean login() {
        if (sshClient.isAuthenticated() && sshClient.isConnected()) {
            return true;
        }
        try{
            sshClient.loadKnownHosts();
            if (!sshClient.isConnected()) {
                sshClient.connect(this.host, this.port);
                logger.info("connect ======> {}", sshClient.isConnected() + "");
            }
            if (!sshClient.isAuthenticated()) {
                sshClient.authPassword(this.username, this.password);
                logger.info("authPassword ======> {}", sshClient.isAuthenticated() + "");
            }
            return true;
        } catch (Exception ex) {
            logger.warn(ex.getMessage(), ex);
            return false;
        }
    }


    /**
     * SCP下载文件
     * @param remotePath 远程目录   如: tmp
     * @param localPath  本地目录， 如： /tmp
     */
    public void download(String remotePath, String localPath) {
        if (!login()) {
            return;
        }
        try{
            if (scpFileTransfer == null) {
                scpFileTransfer = sshClient.newSCPFileTransfer();
            }
            scpFileTransfer.download(remotePath, new FileSystemFile(localPath));
        } catch (Exception ex) {
            logger.warn(ex.getMessage(), ex);
        }
    }

    /**
     * SCP上传文件
     * @param remotePath 远程目录   如: /tmp
     * @param localPath  本地目录， 如： /tmp
     */
    public void upload(String remotePath, String localPath) {
        if (!login()) {
            return;
        }
        try{
            if (scpFileTransfer == null) {
                scpFileTransfer = sshClient.newSCPFileTransfer();
            }
            scpFileTransfer.upload(new FileSystemFile(localPath), remotePath);
        } catch (Exception ex) {
            logger.warn(ex.getMessage(), ex);
        }
    }

    /**
     * SFTP下载文件
     * @param remotePath 远程目录   如: tmp
     * @param localPath  本地目录， 如： /tmp
     */
    public void get(String remotePath, String localPath) {
        if (!login()) {
            return;
        }
        try{
            if (sftp == null) {
                sftp = sshClient.newSFTPClient();
            }
            sftp.get(remotePath, new FileSystemFile(localPath));
        } catch (Exception ex) {
            logger.warn(ex.getMessage(), ex);
        }
    }

    /**
     * SFTP上传文件
     * @param remotePath 远程目录   如: /tmp
     * @param localPath  本地目录， 如： /tmp
     */
    public void put(String remotePath, String localPath) {
        if (!login()) {
            return;
        }
        try{
            if (sftp == null) {
                sftp = sshClient.newSFTPClient();
            }
            sftp.put(new FileSystemFile(localPath), remotePath);
        } catch (Exception ex) {
            logger.warn(ex.getMessage(), ex);
        }
    }

    /**
     * 执行shell命令
     * @param cmdContent shell命令行, 多行命令使用";"分割, 例如: "ping -c 1 www.baidu.com;cd /mnt;ls ./"
     */
    public String exec(String cmdContent) {
        if (!login()) {
            return null;
        }
        Session session = null;
        try{
            session = sshClient.startSession();
            final Session.Command cmd = session.exec(cmdContent);
            String resultLog = IOUtils.readFully(cmd.getInputStream()).toString();
            logger.info(resultLog);
            // 等待5秒后channel关闭
            cmd.join(5, TimeUnit.SECONDS);
            logger.info("\n** exit status: " + cmd.getExitStatus());
            if (cmd.getExitStatus() == 0) {
                return resultLog;
            }
        } catch (Exception ex) {
            logger.warn(ex.getMessage(), ex);
        }
        return null;
    }

    private void closeSession(Session session){
        try {
            if (session != null) {
                session.close();
            }
        } catch (IOException e) { }
    }

    /**
     * 释放连接
     */
    public void disconnect() {
        try {
            if (sshClient != null) {
                sshClient.disconnect();
            }
        } catch (IOException ignored) { }
    }

    /**
     * 释放所有资源
     */
    @Override
    public void close() throws IOException {
        try {
            if (sftp != null) {
                sftp.close();
                sftp = null;
            }
        } catch (IOException ignored) { }

        if (scpFileTransfer != null) {
            scpFileTransfer = null;
        }

        try {
            if (sshClient != null) {
                sshClient.disconnect();
                sshClient = null;
            }
        } catch (IOException ignored) { }
    }

//    public static void main(String[] args) {
//        try(SSHUtil sshUtil = SSHUtil.createInstance("103.74.192.212", "root", "xxxx");){
//            sshUtil.exec("ping -c 1 www.baidu.com;cd /mnt;ls .");
//
//            sshUtil.exec("cd /home;docker ps");
//        } catch (Exception ex) {
//            log.error(ex.getMessage(), ex);
//        }
//
//    }
}
