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

package org.apache.seatunnel.tools.x2seatunnel.parser;

import org.apache.seatunnel.tools.x2seatunnel.model.DataXConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/** DataX JSON配置解析器 */
public class DataXConfigParser {

    private static final Logger logger = LoggerFactory.getLogger(DataXConfigParser.class);
    private final ObjectMapper objectMapper;

    public DataXConfigParser() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * 解析DataX JSON配置文件
     *
     * @param jsonContent JSON内容
     * @return DataX配置对象
     * @throws IllegalArgumentException 如果配置格式无效
     */
    public DataXConfig parse(String jsonContent) {
        try {
            logger.info("开始解析DataX JSON配置");
            JsonNode rootNode = objectMapper.readTree(jsonContent);

            // 验证基本结构
            if (!rootNode.has("job")) {
                throw new IllegalArgumentException("DataX配置缺少必需的 'job' 节点");
            }

            JsonNode jobNode = rootNode.get("job");
            DataXConfig config = new DataXConfig();

            // 解析 job 设置
            if (jobNode.has("setting")) {
                parseJobSetting(jobNode.get("setting"), config);
            }

            // 解析 content 内容
            if (jobNode.has("content")) {
                parseJobContent(jobNode.get("content"), config);
            }

            logger.info("DataX配置解析完成");
            return config;

        } catch (IOException e) {
            logger.error("JSON解析失败: {}", e.getMessage());
            throw new IllegalArgumentException("无效的JSON格式: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("配置解析失败: {}", e.getMessage());
            throw new IllegalArgumentException("DataX配置解析失败: " + e.getMessage(), e);
        }
    }

    /** 解析 job.setting 配置 */
    private void parseJobSetting(JsonNode settingNode, DataXConfig config) {
        logger.debug("解析job.setting配置");

        if (settingNode.has("speed")) {
            JsonNode speedNode = settingNode.get("speed");
            if (speedNode.has("channel")) {
                config.setChannelCount(speedNode.get("channel").asInt());
            }
        }
    }

    /** 解析 job.content 配置 */
    private void parseJobContent(JsonNode contentNode, DataXConfig config) {
        logger.debug("解析job.content配置");

        if (!contentNode.isArray() || contentNode.size() == 0) {
            throw new IllegalArgumentException("DataX配置的 'content' 必须是非空数组");
        }

        // 目前只处理第一个content项
        JsonNode firstContent = contentNode.get(0);

        // 解析reader
        if (firstContent.has("reader")) {
            parseReader(firstContent.get("reader"), config);
        } else {
            throw new IllegalArgumentException("DataX配置缺少必需的 'reader' 配置");
        }

        // 解析writer
        if (firstContent.has("writer")) {
            parseWriter(firstContent.get("writer"), config);
        } else {
            throw new IllegalArgumentException("DataX配置缺少必需的 'writer' 配置");
        }
    }

    /** 解析reader配置 */
    private void parseReader(JsonNode readerNode, DataXConfig config) {
        logger.debug("解析reader配置");

        String readerName = readerNode.get("name").asText();
        config.setReaderName(readerName);

        if (readerNode.has("parameter")) {
            JsonNode paramNode = readerNode.get("parameter");

            // 根据不同的reader类型解析参数
            switch (readerName.toLowerCase()) {
                case "mysqlreader":
                    parseMysqlReaderParams(paramNode, config);
                    break;
                case "oraclereader":
                    parseOracleReaderParams(paramNode, config);
                    break;
                default:
                    parseGenericReaderParams(paramNode, config);
                    break;
            }
        }
    }

    /** 解析MySQL Reader参数 */
    private void parseMysqlReaderParams(JsonNode paramNode, DataXConfig config) {
        if (paramNode.has("username")) {
            config.setReaderUsername(paramNode.get("username").asText());
        }
        if (paramNode.has("password")) {
            config.setReaderPassword(paramNode.get("password").asText());
        }
        if (paramNode.has("connection") && paramNode.get("connection").isArray()) {
            JsonNode connNode = paramNode.get("connection").get(0);
            if (connNode.has("jdbcUrl") && connNode.get("jdbcUrl").isArray()) {
                config.setReaderJdbcUrl(connNode.get("jdbcUrl").get(0).asText());
            }
            if (connNode.has("table") && connNode.get("table").isArray()) {
                config.setReaderTable(connNode.get("table").get(0).asText());
            }
        }
        if (paramNode.has("column")) {
            // 简化处理：将列信息转换为字符串
            config.setReaderColumns(paramNode.get("column").toString());
        }
    }

    /** 解析Oracle Reader参数 */
    private void parseOracleReaderParams(JsonNode paramNode, DataXConfig config) {
        // 与MySQL类似的处理逻辑
        parseMysqlReaderParams(paramNode, config);
    }

    /** 解析通用Reader参数 */
    private void parseGenericReaderParams(JsonNode paramNode, DataXConfig config) {
        // 将所有参数存储为通用属性
        config.addReaderParam("rawParams", paramNode.toString());
    }

    /** 解析writer配置 */
    private void parseWriter(JsonNode writerNode, DataXConfig config) {
        logger.debug("解析writer配置");

        String writerName = writerNode.get("name").asText();
        config.setWriterName(writerName);

        if (writerNode.has("parameter")) {
            JsonNode paramNode = writerNode.get("parameter");

            // 根据不同的writer类型解析参数
            switch (writerName.toLowerCase()) {
                case "txtfilewriter":
                    parseTxtFileWriterParams(paramNode, config);
                    break;
                case "hdfswriter":
                    parseHdfsWriterParams(paramNode, config);
                    break;
                case "hivewriter":
                    parseHiveWriterParams(paramNode, config);
                    break;
                default:
                    parseGenericWriterParams(paramNode, config);
                    break;
            }
        }
    }

    /** 解析TxtFile Writer参数 */
    private void parseTxtFileWriterParams(JsonNode paramNode, DataXConfig config) {
        if (paramNode.has("path")) {
            config.setWriterPath(paramNode.get("path").asText());
        }
        if (paramNode.has("fileName")) {
            config.setWriterFileName(paramNode.get("fileName").asText());
        }
        if (paramNode.has("writeMode")) {
            config.setWriterWriteMode(paramNode.get("writeMode").asText());
        }
        if (paramNode.has("fieldDelimiter")) {
            config.setWriterFieldDelimiter(paramNode.get("fieldDelimiter").asText());
        }
    }

    /** 解析HDFS Writer参数 */
    private void parseHdfsWriterParams(JsonNode paramNode, DataXConfig config) {
        parseTxtFileWriterParams(paramNode, config); // 文件相关参数相似
        if (paramNode.has("defaultFS")) {
            config.addWriterParam("defaultFS", paramNode.get("defaultFS").asText());
        }
    }

    /** 解析Hive Writer参数 */
    private void parseHiveWriterParams(JsonNode paramNode, DataXConfig config) {
        if (paramNode.has("metastoreUris")) {
            config.addWriterParam("metastoreUris", paramNode.get("metastoreUris").asText());
        }
        if (paramNode.has("database")) {
            config.addWriterParam("database", paramNode.get("database").asText());
        }
        if (paramNode.has("table")) {
            config.setWriterTable(paramNode.get("table").asText());
        }
    }

    /** 解析通用Writer参数 */
    private void parseGenericWriterParams(JsonNode paramNode, DataXConfig config) {
        // 将所有参数存储为通用属性
        config.addWriterParam("rawParams", paramNode.toString());
    }
}
