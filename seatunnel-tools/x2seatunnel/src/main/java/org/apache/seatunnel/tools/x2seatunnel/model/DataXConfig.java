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

package org.apache.seatunnel.tools.x2seatunnel.model;

import java.util.HashMap;
import java.util.Map;

/** DataX配置数据模型 */
public class DataXConfig {

    // Job 设置
    private int channelCount = 1;

    // Reader 配置
    private String readerName;
    private String readerUsername;
    private String readerPassword;
    private String readerJdbcUrl;
    private String readerTable;
    private String readerColumns;
    private Map<String, Object> readerParams = new HashMap<>();

    // Writer 配置
    private String writerName;
    private String writerPath;
    private String writerFileName;
    private String writerWriteMode;
    private String writerFieldDelimiter;
    private String writerTable;
    private Map<String, Object> writerParams = new HashMap<>();

    // Getter and Setter methods

    public int getChannelCount() {
        return channelCount;
    }

    public void setChannelCount(int channelCount) {
        this.channelCount = channelCount;
    }

    public String getReaderName() {
        return readerName;
    }

    public void setReaderName(String readerName) {
        this.readerName = readerName;
    }

    public String getReaderUsername() {
        return readerUsername;
    }

    public void setReaderUsername(String readerUsername) {
        this.readerUsername = readerUsername;
    }

    public String getReaderPassword() {
        return readerPassword;
    }

    public void setReaderPassword(String readerPassword) {
        this.readerPassword = readerPassword;
    }

    public String getReaderJdbcUrl() {
        return readerJdbcUrl;
    }

    public void setReaderJdbcUrl(String readerJdbcUrl) {
        this.readerJdbcUrl = readerJdbcUrl;
    }

    public String getReaderTable() {
        return readerTable;
    }

    public void setReaderTable(String readerTable) {
        this.readerTable = readerTable;
    }

    public String getReaderColumns() {
        return readerColumns;
    }

    public void setReaderColumns(String readerColumns) {
        this.readerColumns = readerColumns;
    }

    public Map<String, Object> getReaderParams() {
        return readerParams;
    }

    public void addReaderParam(String key, Object value) {
        this.readerParams.put(key, value);
    }

    public String getWriterName() {
        return writerName;
    }

    public void setWriterName(String writerName) {
        this.writerName = writerName;
    }

    public String getWriterPath() {
        return writerPath;
    }

    public void setWriterPath(String writerPath) {
        this.writerPath = writerPath;
    }

    public String getWriterFileName() {
        return writerFileName;
    }

    public void setWriterFileName(String writerFileName) {
        this.writerFileName = writerFileName;
    }

    public String getWriterWriteMode() {
        return writerWriteMode;
    }

    public void setWriterWriteMode(String writerWriteMode) {
        this.writerWriteMode = writerWriteMode;
    }

    public String getWriterFieldDelimiter() {
        return writerFieldDelimiter;
    }

    public void setWriterFieldDelimiter(String writerFieldDelimiter) {
        this.writerFieldDelimiter = writerFieldDelimiter;
    }

    public String getWriterTable() {
        return writerTable;
    }

    public void setWriterTable(String writerTable) {
        this.writerTable = writerTable;
    }

    public Map<String, Object> getWriterParams() {
        return writerParams;
    }

    public void addWriterParam(String key, Object value) {
        this.writerParams.put(key, value);
    }

    @Override
    public String toString() {
        return "DataXConfig{"
                + "channelCount="
                + channelCount
                + ", readerName='"
                + readerName
                + '\''
                + ", readerUsername='"
                + readerUsername
                + '\''
                + ", readerJdbcUrl='"
                + readerJdbcUrl
                + '\''
                + ", readerTable='"
                + readerTable
                + '\''
                + ", writerName='"
                + writerName
                + '\''
                + ", writerPath='"
                + writerPath
                + '\''
                + ", writerFileName='"
                + writerFileName
                + '\''
                + '}';
    }
}
