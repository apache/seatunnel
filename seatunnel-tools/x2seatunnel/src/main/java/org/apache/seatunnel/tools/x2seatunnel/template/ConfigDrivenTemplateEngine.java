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

package org.apache.seatunnel.tools.x2seatunnel.template;

import org.apache.seatunnel.tools.x2seatunnel.model.DataXConfig;
import org.apache.seatunnel.tools.x2seatunnel.model.MappingResult;
import org.apache.seatunnel.tools.x2seatunnel.util.FileUtils;
import org.apache.seatunnel.tools.x2seatunnel.util.PathResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 配置驱动的模板转换引擎 基于template-mapping.yaml配置文件自动选择和应用模板 */
public class ConfigDrivenTemplateEngine {

    private static final Logger logger = LoggerFactory.getLogger(ConfigDrivenTemplateEngine.class);

    private final TemplateMappingManager mappingManager;
    private final TemplateVariableResolver variableResolver;

    public ConfigDrivenTemplateEngine() {
        this.mappingManager = TemplateMappingManager.getInstance();
        this.variableResolver = new TemplateVariableResolver(this.mappingManager);
    }

    /**
     * 使用配置驱动的方式转换DataX配置
     *
     * @param dataXConfig DataX配置对象
     * @param sourceContent 原始DataX JSON内容
     * @return 转换结果
     */
    public TemplateConversionResult convertWithTemplate(
            DataXConfig dataXConfig, String sourceContent) {
        logger.info("开始配置驱动的模板转换...");

        TemplateConversionResult result = new TemplateConversionResult();

        try {
            // 1. 根据reader类型选择source模板
            String readerType = dataXConfig.getReaderName();
            String sourceTemplate = mappingManager.getSourceTemplate(readerType);
            logger.info("为reader类型 {} 选择source模板: {}", readerType, sourceTemplate);

            // 2. 根据writer类型选择sink模板
            String writerType = dataXConfig.getWriterName();
            String sinkTemplate = mappingManager.getSinkTemplate(writerType);
            logger.info("为writer类型 {} 选择sink模板: {}", writerType, sinkTemplate);

            // 3. 加载模板内容
            String sourceTemplateContent = loadTemplate(sourceTemplate);
            String sinkTemplateContent = loadTemplate(sinkTemplate);

            // 4. 生成env配置
            String envConfig = generateEnvConfig(dataXConfig);

            // 5. 使用变量解析器处理source模板
            String resolvedSourceConfig =
                    variableResolver.resolve(sourceTemplateContent, sourceContent);

            // 6. 使用变量解析器处理sink模板
            String resolvedSinkConfig =
                    variableResolver.resolve(sinkTemplateContent, sourceContent);

            // 7. 组装完整的SeaTunnel配置
            String finalConfig =
                    assembleConfig(envConfig, resolvedSourceConfig, resolvedSinkConfig);

            // 8. 生成映射结果（用于报告）
            MappingResult mappingResult =
                    generateMappingResult(
                            dataXConfig, readerType, writerType, sourceTemplate, sinkTemplate);

            result.setSuccess(true);
            result.setConfigContent(finalConfig);
            result.setMappingResult(mappingResult);
            result.setSourceTemplate(sourceTemplate);
            result.setSinkTemplate(sinkTemplate);

            logger.info("配置驱动的模板转换完成");

        } catch (Exception e) {
            logger.error("配置驱动的模板转换失败: {}", e.getMessage(), e);
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        }

        return result;
    }

    /** 加载模板文件内容 */
    private String loadTemplate(String templatePath) {
        logger.debug("加载模板文件: {}", templatePath);

        // 1. 尝试从文件系统加载
        String resolvedPath = PathResolver.resolveTemplatePath(templatePath);
        if (resolvedPath != null && PathResolver.exists(resolvedPath)) {
            logger.debug("从文件系统加载模板: {}", resolvedPath);
            return FileUtils.readFile(resolvedPath);
        }

        // 2. 从classpath加载（内置模板）
        try {
            String resourcePath = PathResolver.buildResourcePath(templatePath);
            logger.debug("从classpath加载模板: {}", resourcePath);
            return FileUtils.readResourceFile(resourcePath);
        } catch (Exception e) {
            throw new RuntimeException("无法加载模板文件: " + templatePath, e);
        }
    }

    /** 生成env配置部分 */
    private String generateEnvConfig(DataXConfig dataXConfig) {
        StringBuilder envConfig = new StringBuilder();
        envConfig.append("env {\n");

        // 并行度配置
        int parallelism = dataXConfig.getChannelCount() > 0 ? dataXConfig.getChannelCount() : 1;
        envConfig.append("  parallelism = ").append(parallelism).append("\n");

        // 作业模式
        envConfig.append("  job.mode = \"BATCH\"\n");

        envConfig.append("}\n");
        return envConfig.toString();
    }

    /** 组装完整的SeaTunnel配置 */
    private String assembleConfig(String envConfig, String sourceConfig, String sinkConfig) {
        StringBuilder finalConfig = new StringBuilder();

        // 添加头部注释
        finalConfig.append("# SeaTunnel配置文件\n");
        finalConfig.append("# 由X2SeaTunnel配置驱动引擎自动生成\n");
        finalConfig.append("# 生成时间: ").append(java.time.LocalDateTime.now()).append("\n");
        finalConfig.append("\n");

        // 添加env配置
        finalConfig.append(envConfig).append("\n");

        // 添加source配置
        finalConfig.append(sourceConfig).append("\n");

        // 添加sink配置
        finalConfig.append(sinkConfig).append("\n");

        return finalConfig.toString();
    }

    /** 生成映射结果（用于报告生成） */
    private MappingResult generateMappingResult(
            DataXConfig dataXConfig,
            String readerType,
            String writerType,
            String sourceTemplate,
            String sinkTemplate) {
        MappingResult result = new MappingResult();

        // 添加成功映射
        result.addSuccessMapping("reader.name", "source.template", sourceTemplate);
        result.addSuccessMapping("writer.name", "sink.template", sinkTemplate);

        // 添加并行度映射
        if (dataXConfig.getChannelCount() > 0) {
            result.addSuccessMapping(
                    "speed.channel",
                    "env.parallelism",
                    String.valueOf(dataXConfig.getChannelCount()));
        } else {
            result.addAutoConstructedField("env.parallelism", "1", "使用默认并行度");
        }

        // 添加作业模式
        result.addAutoConstructedField("env.job.mode", "BATCH", "DataX默认为批处理模式");

        // 检查是否支持的类型
        if (!mappingManager.isReaderSupported(readerType)) {
            result.addUnmappedField("reader.name", readerType, "使用默认JDBC模板");
        }

        if (!mappingManager.isWriterSupported(writerType)) {
            result.addUnmappedField("writer.name", writerType, "使用默认HDFS模板");
        }

        result.setSuccess(true);
        return result;
    }

    /** 检查是否支持指定的配置组合 */
    public boolean isConfigurationSupported(String readerType, String writerType) {
        return mappingManager.isReaderSupported(readerType)
                && mappingManager.isWriterSupported(writerType);
    }

    /** 获取支持的配置信息 */
    public String getSupportedConfigInfo() {
        StringBuilder info = new StringBuilder();
        info.append("支持的Reader类型: ");
        info.append(String.join(", ", mappingManager.getSupportedReaders()));
        info.append("\n");
        info.append("支持的Writer类型: ");
        info.append(String.join(", ", mappingManager.getSupportedWriters()));
        return info.toString();
    }

    /** 模板转换结果类 */
    public static class TemplateConversionResult {
        private boolean success;
        private String configContent;
        private String errorMessage;
        private MappingResult mappingResult;
        private String sourceTemplate;
        private String sinkTemplate;

        // Getters and setters
        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getConfigContent() {
            return configContent;
        }

        public void setConfigContent(String configContent) {
            this.configContent = configContent;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public MappingResult getMappingResult() {
            return mappingResult;
        }

        public void setMappingResult(MappingResult mappingResult) {
            this.mappingResult = mappingResult;
        }

        public String getSourceTemplate() {
            return sourceTemplate;
        }

        public void setSourceTemplate(String sourceTemplate) {
            this.sourceTemplate = sourceTemplate;
        }

        public String getSinkTemplate() {
            return sinkTemplate;
        }

        public void setSinkTemplate(String sinkTemplate) {
            this.sinkTemplate = sinkTemplate;
        }
    }
}
