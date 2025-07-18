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

package org.apache.seatunnel.tools.x2seatunnel.mapping;

import org.apache.seatunnel.tools.x2seatunnel.model.DataXConfig;
import org.apache.seatunnel.tools.x2seatunnel.model.MappingResult;
import org.apache.seatunnel.tools.x2seatunnel.model.SeaTunnelConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 映射规则引擎核心类 */
public class MappingRuleEngine {

    private static final Logger logger = LoggerFactory.getLogger(MappingRuleEngine.class);

    /**
     * 执行DataX到SeaTunnel的配置映射
     *
     * @param dataXConfig DataX配置
     * @return 映射结果
     */
    public MappingResult mapToSeaTunnel(DataXConfig dataXConfig) {
        logger.info("开始执行DataX到SeaTunnel的配置映射");

        MappingResult result = new MappingResult();
        SeaTunnelConfig seaTunnelConfig = new SeaTunnelConfig();

        try {
            // 映射环境配置
            mapEnvironmentConfig(dataXConfig, seaTunnelConfig, result);

            // 映射Source配置
            mapSourceConfig(dataXConfig, seaTunnelConfig, result);

            // 映射Sink配置
            mapSinkConfig(dataXConfig, seaTunnelConfig, result);

            result.setSeaTunnelConfig(seaTunnelConfig);
            result.setSuccess(true);

            logger.info(
                    "配置映射完成，成功: {}, 自动构造: {}, 缺失: {}",
                    result.getSuccessMappings().size(),
                    result.getAutoConstructedFields().size(),
                    result.getMissingRequiredFields().size());

        } catch (Exception e) {
            logger.error("配置映射失败: {}", e.getMessage(), e);
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        }

        return result;
    }

    /** 映射环境配置 */
    private void mapEnvironmentConfig(
            DataXConfig dataXConfig, SeaTunnelConfig seaTunnelConfig, MappingResult result) {
        logger.debug("映射环境配置");

        // 映射并行度
        if (dataXConfig.getChannelCount() > 0) {
            seaTunnelConfig.setParallelism(dataXConfig.getChannelCount());
            result.addSuccessMapping(
                    "speed.channel",
                    "env.parallelism",
                    String.valueOf(dataXConfig.getChannelCount()));
        } else {
            // 设置默认并行度
            seaTunnelConfig.setParallelism(1);
            result.addAutoConstructedField("env.parallelism", "1", "使用默认并行度");
        }

        // 设置作业模式为批处理（默认）
        seaTunnelConfig.setJobMode("BATCH");
        result.addAutoConstructedField("env.job.mode", "BATCH", "DataX默认为批处理模式");
    }

    /** 映射Source配置 */
    private void mapSourceConfig(
            DataXConfig dataXConfig, SeaTunnelConfig seaTunnelConfig, MappingResult result) {
        logger.debug("映射Source配置，reader: {}", dataXConfig.getReaderName());

        String readerName = dataXConfig.getReaderName();
        if (readerName == null || readerName.isEmpty()) {
            result.addMissingRequiredField("reader.name", "必须指定reader类型");
            return;
        }

        switch (readerName.toLowerCase()) {
            case "mysqlreader":
                mapMysqlSource(dataXConfig, seaTunnelConfig, result);
                break;
            case "postgresqlreader":
                mapPostgreSqlSource(dataXConfig, seaTunnelConfig, result);
                break;
            case "oraclereader":
                mapOracleSource(dataXConfig, seaTunnelConfig, result);
                break;
            case "sqlserverreader":
                mapSqlServerSource(dataXConfig, seaTunnelConfig, result);
                break;
            default:
                mapGenericSource(dataXConfig, seaTunnelConfig, result);
                break;
        }
    }

    /** 映射MySQL Source */
    private void mapMysqlSource(
            DataXConfig dataXConfig, SeaTunnelConfig seaTunnelConfig, MappingResult result) {
        seaTunnelConfig.setSourceType("Jdbc");
        result.addSuccessMapping("reader.name", "source.type", "Jdbc");

        // 映射数据库连接信息
        if (dataXConfig.getReaderJdbcUrl() != null) {
            seaTunnelConfig.setSourceUrl(dataXConfig.getReaderJdbcUrl());
            result.addSuccessMapping(
                    "reader.parameter.connection.jdbcUrl",
                    "source.url",
                    dataXConfig.getReaderJdbcUrl());
        } else {
            result.addMissingRequiredField("source.url", "缺少JDBC连接URL");
        }

        if (dataXConfig.getReaderUsername() != null) {
            seaTunnelConfig.setSourceUser(dataXConfig.getReaderUsername());
            result.addSuccessMapping(
                    "reader.parameter.username", "source.user", dataXConfig.getReaderUsername());
        }

        if (dataXConfig.getReaderPassword() != null) {
            seaTunnelConfig.setSourcePassword(dataXConfig.getReaderPassword());
            result.addSuccessMapping(
                    "reader.parameter.password",
                    "source.password",
                    dataXConfig.getReaderPassword());
        }

        // 设置驱动程序
        seaTunnelConfig.setSourceDriver("com.mysql.cj.jdbc.Driver");
        result.addAutoConstructedField("source.driver", "com.mysql.cj.jdbc.Driver", "MySQL默认驱动");

        // 构造查询语句
        if (dataXConfig.getReaderTable() != null) {
            String query = "SELECT * FROM " + dataXConfig.getReaderTable();
            seaTunnelConfig.setSourceQuery(query);
            result.addAutoConstructedField("source.query", query, "根据表名自动构造查询语句");
        }
    }

    /** 映射Oracle Source */
    private void mapOracleSource(
            DataXConfig dataXConfig, SeaTunnelConfig seaTunnelConfig, MappingResult result) {
        seaTunnelConfig.setSourceType("Jdbc");
        result.addSuccessMapping("reader.name", "source.type", "Jdbc");

        // Oracle的处理逻辑与MySQL类似，但使用不同的驱动
        if (dataXConfig.getReaderJdbcUrl() != null) {
            seaTunnelConfig.setSourceUrl(dataXConfig.getReaderJdbcUrl());
            result.addSuccessMapping(
                    "reader.parameter.connection.jdbcUrl",
                    "source.url",
                    dataXConfig.getReaderJdbcUrl());
        }

        if (dataXConfig.getReaderUsername() != null) {
            seaTunnelConfig.setSourceUser(dataXConfig.getReaderUsername());
            result.addSuccessMapping(
                    "reader.parameter.username", "source.user", dataXConfig.getReaderUsername());
        }

        if (dataXConfig.getReaderPassword() != null) {
            seaTunnelConfig.setSourcePassword(dataXConfig.getReaderPassword());
            result.addSuccessMapping(
                    "reader.parameter.password",
                    "source.password",
                    dataXConfig.getReaderPassword());
        }

        // Oracle驱动
        seaTunnelConfig.setSourceDriver("oracle.jdbc.driver.OracleDriver");
        result.addAutoConstructedField(
                "source.driver", "oracle.jdbc.driver.OracleDriver", "Oracle默认驱动");

        if (dataXConfig.getReaderTable() != null) {
            String query = "SELECT * FROM " + dataXConfig.getReaderTable();
            seaTunnelConfig.setSourceQuery(query);
            result.addAutoConstructedField("source.query", query, "根据表名自动构造查询语句");
        }
    }

    /** 映射PostgreSQL Source */
    private void mapPostgreSqlSource(
            DataXConfig dataXConfig, SeaTunnelConfig seaTunnelConfig, MappingResult result) {
        seaTunnelConfig.setSourceType("Jdbc");
        result.addSuccessMapping("reader.name", "source.type", "Jdbc");

        // 映射数据库连接信息
        if (dataXConfig.getReaderJdbcUrl() != null) {
            seaTunnelConfig.setSourceUrl(dataXConfig.getReaderJdbcUrl());
            result.addSuccessMapping(
                    "reader.parameter.connection.jdbcUrl",
                    "source.url",
                    dataXConfig.getReaderJdbcUrl());
        } else {
            result.addMissingRequiredField("source.url", "缺少JDBC连接URL");
        }

        if (dataXConfig.getReaderUsername() != null) {
            seaTunnelConfig.setSourceUser(dataXConfig.getReaderUsername());
            result.addSuccessMapping(
                    "reader.parameter.username", "source.user", dataXConfig.getReaderUsername());
        }

        if (dataXConfig.getReaderPassword() != null) {
            seaTunnelConfig.setSourcePassword(dataXConfig.getReaderPassword());
            result.addSuccessMapping(
                    "reader.parameter.password",
                    "source.password",
                    dataXConfig.getReaderPassword());
        }

        // PostgreSQL驱动
        seaTunnelConfig.setSourceDriver("org.postgresql.Driver");
        result.addAutoConstructedField("source.driver", "org.postgresql.Driver", "PostgreSQL默认驱动");

        // 构造查询语句
        if (dataXConfig.getReaderTable() != null) {
            String query = "SELECT * FROM " + dataXConfig.getReaderTable();
            seaTunnelConfig.setSourceQuery(query);
            result.addAutoConstructedField("source.query", query, "根据表名自动构造查询语句");
        }
    }

    /** 映射SQL Server Source */
    private void mapSqlServerSource(
            DataXConfig dataXConfig, SeaTunnelConfig seaTunnelConfig, MappingResult result) {
        seaTunnelConfig.setSourceType("Jdbc");
        result.addSuccessMapping("reader.name", "source.type", "Jdbc");

        // 映射数据库连接信息
        if (dataXConfig.getReaderJdbcUrl() != null) {
            seaTunnelConfig.setSourceUrl(dataXConfig.getReaderJdbcUrl());
            result.addSuccessMapping(
                    "reader.parameter.connection.jdbcUrl",
                    "source.url",
                    dataXConfig.getReaderJdbcUrl());
        } else {
            result.addMissingRequiredField("source.url", "缺少JDBC连接URL");
        }

        if (dataXConfig.getReaderUsername() != null) {
            seaTunnelConfig.setSourceUser(dataXConfig.getReaderUsername());
            result.addSuccessMapping(
                    "reader.parameter.username", "source.user", dataXConfig.getReaderUsername());
        }

        if (dataXConfig.getReaderPassword() != null) {
            seaTunnelConfig.setSourcePassword(dataXConfig.getReaderPassword());
            result.addSuccessMapping(
                    "reader.parameter.password",
                    "source.password",
                    dataXConfig.getReaderPassword());
        }

        // SQL Server驱动
        seaTunnelConfig.setSourceDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        result.addAutoConstructedField(
                "source.driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver", "SQL Server默认驱动");

        // 构造查询语句
        if (dataXConfig.getReaderTable() != null) {
            String query = "SELECT * FROM " + dataXConfig.getReaderTable();
            seaTunnelConfig.setSourceQuery(query);
            result.addAutoConstructedField("source.query", query, "根据表名自动构造查询语句");
        }
    }

    /** 映射通用Source */
    private void mapGenericSource(
            DataXConfig dataXConfig, SeaTunnelConfig seaTunnelConfig, MappingResult result) {
        // 对于不支持的reader类型，设置为Console用于演示
        seaTunnelConfig.setSourceType("Console");
        result.addUnmappedField(
                "reader.name", dataXConfig.getReaderName(), "不支持的reader类型，使用Console替代");
    }

    /** 映射Sink配置 */
    private void mapSinkConfig(
            DataXConfig dataXConfig, SeaTunnelConfig seaTunnelConfig, MappingResult result) {
        logger.debug("映射Sink配置，writer: {}", dataXConfig.getWriterName());

        String writerName = dataXConfig.getWriterName();
        if (writerName == null || writerName.isEmpty()) {
            result.addMissingRequiredField("writer.name", "必须指定writer类型");
            return;
        }

        switch (writerName.toLowerCase()) {
            case "txtfilewriter":
                mapTextFileSink(dataXConfig, seaTunnelConfig, result);
                break;
            case "hdfswriter":
                mapHdfsSink(dataXConfig, seaTunnelConfig, result);
                break;
            case "hivewriter":
                mapHiveSink(dataXConfig, seaTunnelConfig, result);
                break;
            default:
                mapGenericSink(dataXConfig, seaTunnelConfig, result);
                break;
        }
    }

    /** 映射文本文件Sink */
    private void mapTextFileSink(
            DataXConfig dataXConfig, SeaTunnelConfig seaTunnelConfig, MappingResult result) {
        seaTunnelConfig.setSinkType("LocalFile");
        result.addSuccessMapping("writer.name", "sink.type", "LocalFile");

        if (dataXConfig.getWriterPath() != null) {
            seaTunnelConfig.setSinkPath(dataXConfig.getWriterPath());
            result.addSuccessMapping(
                    "writer.parameter.path", "sink.path", dataXConfig.getWriterPath());
        }

        if (dataXConfig.getWriterFileName() != null) {
            seaTunnelConfig.setSinkFileName(dataXConfig.getWriterFileName());
            result.addSuccessMapping(
                    "writer.parameter.fileName",
                    "sink.file_name_expression",
                    dataXConfig.getWriterFileName());
        }

        if (dataXConfig.getWriterFieldDelimiter() != null) {
            seaTunnelConfig.setSinkFieldDelimiter(dataXConfig.getWriterFieldDelimiter());
            result.addSuccessMapping(
                    "writer.parameter.fieldDelimiter",
                    "sink.field_delimiter",
                    dataXConfig.getWriterFieldDelimiter());
        }

        // 设置默认文件格式
        seaTunnelConfig.setSinkFileFormat("text");
        result.addAutoConstructedField("sink.file_format", "text", "文本文件默认格式");
    }

    /** 映射HDFS Sink */
    private void mapHdfsSink(
            DataXConfig dataXConfig, SeaTunnelConfig seaTunnelConfig, MappingResult result) {
        seaTunnelConfig.setSinkType("HdfsFile");
        result.addSuccessMapping("writer.name", "sink.type", "HdfsFile");

        if (dataXConfig.getWriterPath() != null) {
            seaTunnelConfig.setSinkPath(dataXConfig.getWriterPath());
            result.addSuccessMapping(
                    "writer.parameter.path", "sink.path", dataXConfig.getWriterPath());
        }

        // HDFS特有配置
        Object defaultFS = dataXConfig.getWriterParams().get("defaultFS");
        if (defaultFS != null) {
            seaTunnelConfig.addSinkParam("fs.defaultFS", defaultFS.toString());
            result.addSuccessMapping(
                    "writer.parameter.defaultFS", "sink.fs.defaultFS", defaultFS.toString());
        }
    }

    /** 映射Hive Sink */
    private void mapHiveSink(
            DataXConfig dataXConfig, SeaTunnelConfig seaTunnelConfig, MappingResult result) {
        seaTunnelConfig.setSinkType("Hive");
        result.addSuccessMapping("writer.name", "sink.type", "Hive");

        if (dataXConfig.getWriterTable() != null) {
            seaTunnelConfig.setSinkTable(dataXConfig.getWriterTable());
            result.addSuccessMapping(
                    "writer.parameter.table", "sink.table_name", dataXConfig.getWriterTable());
        }

        Object metastoreUris = dataXConfig.getWriterParams().get("metastoreUris");
        if (metastoreUris != null) {
            seaTunnelConfig.addSinkParam("metastore_uri", metastoreUris.toString());
            result.addSuccessMapping(
                    "writer.parameter.metastoreUris",
                    "sink.metastore_uri",
                    metastoreUris.toString());
        }
    }

    /** 映射通用Sink */
    private void mapGenericSink(
            DataXConfig dataXConfig, SeaTunnelConfig seaTunnelConfig, MappingResult result) {
        // 对于不支持的writer类型，设置为Console用于演示
        seaTunnelConfig.setSinkType("Console");
        result.addUnmappedField(
                "writer.name", dataXConfig.getWriterName(), "不支持的writer类型，使用Console替代");
    }
}
