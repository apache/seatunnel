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

/** SeaTunnel configuration data model */
public class SeaTunnelConfig {

    // Environment configuration
    private int parallelism = 1;
    private String jobMode = "BATCH";

    // Source configuration
    private String sourceType;
    private String sourceUrl;
    private String sourceUser;
    private String sourcePassword;
    private String sourceDriver;
    private String sourceQuery;
    private Map<String, Object> sourceParams = new HashMap<>();

    // Sink configuration
    private String sinkType;
    private String sinkPath;
    private String sinkFileName;
    private String sinkFieldDelimiter;
    private String sinkFileFormat;
    private String sinkTable;
    private Map<String, Object> sinkParams = new HashMap<>();

    // Getter and Setter methods

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public String getJobMode() {
        return jobMode;
    }

    public void setJobMode(String jobMode) {
        this.jobMode = jobMode;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getSourceUrl() {
        return sourceUrl;
    }

    public void setSourceUrl(String sourceUrl) {
        this.sourceUrl = sourceUrl;
    }

    public String getSourceUser() {
        return sourceUser;
    }

    public void setSourceUser(String sourceUser) {
        this.sourceUser = sourceUser;
    }

    public String getSourcePassword() {
        return sourcePassword;
    }

    public void setSourcePassword(String sourcePassword) {
        this.sourcePassword = sourcePassword;
    }

    public String getSourceDriver() {
        return sourceDriver;
    }

    public void setSourceDriver(String sourceDriver) {
        this.sourceDriver = sourceDriver;
    }

    public String getSourceQuery() {
        return sourceQuery;
    }

    public void setSourceQuery(String sourceQuery) {
        this.sourceQuery = sourceQuery;
    }

    public Map<String, Object> getSourceParams() {
        return sourceParams;
    }

    public void addSourceParam(String key, Object value) {
        this.sourceParams.put(key, value);
    }

    public String getSinkType() {
        return sinkType;
    }

    public void setSinkType(String sinkType) {
        this.sinkType = sinkType;
    }

    public String getSinkPath() {
        return sinkPath;
    }

    public void setSinkPath(String sinkPath) {
        this.sinkPath = sinkPath;
    }

    public String getSinkFileName() {
        return sinkFileName;
    }

    public void setSinkFileName(String sinkFileName) {
        this.sinkFileName = sinkFileName;
    }

    public String getSinkFieldDelimiter() {
        return sinkFieldDelimiter;
    }

    public void setSinkFieldDelimiter(String sinkFieldDelimiter) {
        this.sinkFieldDelimiter = sinkFieldDelimiter;
    }

    public String getSinkFileFormat() {
        return sinkFileFormat;
    }

    public void setSinkFileFormat(String sinkFileFormat) {
        this.sinkFileFormat = sinkFileFormat;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public Map<String, Object> getSinkParams() {
        return sinkParams;
    }

    public void addSinkParam(String key, Object value) {
        this.sinkParams.put(key, value);
    }

    @Override
    public String toString() {
        return "SeaTunnelConfig{"
                + "parallelism="
                + parallelism
                + ", jobMode='"
                + jobMode
                + '\''
                + ", sourceType='"
                + sourceType
                + '\''
                + ", sourceUrl='"
                + sourceUrl
                + '\''
                + ", sourceUser='"
                + sourceUser
                + '\''
                + ", sinkType='"
                + sinkType
                + '\''
                + ", sinkPath='"
                + sinkPath
                + '\''
                + '}';
    }
}
