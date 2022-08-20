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

package org.apache.seatunnel.connectors.seatunnel.file.sink.ftp.util;

import lombok.NonNull;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.util.StringTokenizer;

public class FtpFileUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FtpFileUtils.class);
    public  static FTPClient FTPCLIENT;
    public static  String FTP_PASSWORD;
    public static  String FTP_USERNAME;
    public static  String FTP_HOST;
    public static  Integer FTP_PORT;
    public static final int FTP_CONNECT_MAX_TIMEOUT = 30000;

    public static FTPClient getFTPClient(){
        return getFTPClient(FTP_HOST, FTP_PORT, FTP_USERNAME, FTP_PASSWORD);
    }

    public static FTPClient getFTPClient(String ftpHost, int ftpPort, String ftpUserName, String ftpPassword) {
        try {
            FTPCLIENT = new FTPClient();
            FTPCLIENT.connect(ftpHost, ftpPort);
            FTPCLIENT.login(ftpUserName, ftpPassword);
            FTPCLIENT.setConnectTimeout(FTP_CONNECT_MAX_TIMEOUT);
            FTPCLIENT.setControlEncoding("utf-8");
            FTPCLIENT.enterLocalPassiveMode();

            FTPCLIENT.setFileType(FTPCLIENT.BINARY_FILE_TYPE);

            if (!FTPReply.isPositiveCompletion(FTPCLIENT.getReplyCode())) {
                FTPCLIENT.disconnect();
            }
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return FTPCLIENT;
    }

    public static OutputStream getOutputStream(@NonNull String outFilePath) throws IOException {
        FTPClient ftpClient = FtpFileUtils.getFTPClient();
        if (!fileExist(outFilePath)) {
            createFile(outFilePath);
        }

        OutputStream outputStream = ftpClient.appendFileStream(new String(outFilePath.getBytes("UTF-8"), "iso-8859-1"));
        return outputStream;
    }

    public static boolean createDir(@NonNull String dirPath) throws IOException {
        FTPClient ftpClient = getFTPClient();

        StringTokenizer s = new StringTokenizer(dirPath, "/"); // sign

        s.countTokens();

        String pathName = "";

        while (s.hasMoreElements()) {
            pathName = pathName + "/" +  s.nextElement();
            try {

                ftpClient.makeDirectory(pathName);
            } catch (Exception e) {

                return false;
            }
        }
        return true;
    }

    public static boolean createFile(@NonNull String filePath) throws IOException {
        FTPClient ftpClient = getFTPClient();
        StringTokenizer s = new StringTokenizer(filePath, "/"); // sign
        int c = s.countTokens();
        String dirName = "";
        for (int i = 0; i < c - 1; i++) {
            dirName = dirName + "/" +  s.nextElement();
        }
        createDir(dirName);
        byte[] b = "".getBytes();
        InputStream inputStream = new ByteArrayInputStream(b);
        ftpClient.storeFile(filePath, inputStream);
        return true;
    }

    public static boolean fileExist(@NonNull String filePath) throws IOException {
        FTPClient ftpClient = getFTPClient();
        String[] listNames = ftpClient.listNames(filePath);
        if (listNames != null) {
            return listNames.length > 0;
        }
        else {
            return false;
        }

    }

    public static void renameFile(@NonNull String oldName, @NonNull String newName) throws IOException {
        LOGGER.info("begin rename file oldName :[" + oldName + "] to newName :[" + newName + "]");
        FTPClient ftpClient = getFTPClient();

        if (!FtpFileUtils.fileExist(newName)) {
            FtpFileUtils.createFile(newName);
        } else {
            FtpFileUtils.deleteFile(newName);
        }
        if (ftpClient.rename(oldName, newName)) {
            LOGGER.info("rename file  :[" + oldName + "] to [" + newName + "] finish");
        } else {
            throw new IOException("rename file :[" + oldName + "] to [" + newName + "] error");
        }
    }

    public static void deleteFile(@NonNull String filePath) throws IOException {
        FTPClient ftpClient = getFTPClient();
        ftpClient.deleteFile(filePath);
    }

    public static boolean deleteFiles(@NonNull String pathName) {

        FTPClient ftpClient = getFTPClient();
        try {
            FTPFile[] ftpFiles = ftpClient.listFiles(pathName);
            if (null != ftpFiles && ftpFiles.length > 0) {
                for (int i = 0; i < ftpFiles.length; i++) {
                    FTPFile thisFile = ftpFiles[i];
                    if (thisFile.isDirectory()) {
                        deleteFiles(pathName + "/" + thisFile.getName());

                        ftpClient.removeDirectory(pathName);
                    } else {
                        if (!ftpClient.deleteFile(pathName)) {
                            return false;
                        }
                    }
                }
            }
            ftpClient.removeDirectory(pathName);
        } catch (Exception e) {
            LOGGER.error("delete file [" + pathName + "] error");
            return false;
        }
        return true;
    }
}
