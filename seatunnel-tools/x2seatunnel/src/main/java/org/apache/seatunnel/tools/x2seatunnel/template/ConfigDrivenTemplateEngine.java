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
import org.apache.seatunnel.tools.x2seatunnel.model.MappingTracker;
import org.apache.seatunnel.tools.x2seatunnel.util.FileUtils;
import org.apache.seatunnel.tools.x2seatunnel.util.PathResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 配置驱动的模板转换引擎 基于template-mapping.yaml配置文件自动选择和应用模板 */
public class ConfigDrivenTemplateEngine {

    private static final Logger logger = LoggerFactory.getLogger(ConfigDrivenTemplateEngine.class);

    private final TemplateMappingManager mappingManager;
    private final TemplateVariableResolver variableResolver;
    private final MappingTracker mappingTracker; // 新增：映射跟踪器

    public ConfigDrivenTemplateEngine() {
        this.mappingManager = TemplateMappingManager.getInstance();
        this.mappingTracker = new MappingTracker(); // 初始化映射跟踪器
        this.variableResolver =
                new TemplateVariableResolver(this.mappingManager, this.mappingTracker);
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
            // 重置映射跟踪器状态
            mappingTracker.reset();
            logger.info("映射跟踪器已重置，开始新的转换过程");

            // 创建字段引用跟踪器
            org.apache.seatunnel.tools.x2seatunnel.util.DataXFieldExtractor dataXExtractor =
                    new org.apache.seatunnel.tools.x2seatunnel.util.DataXFieldExtractor();
            org.apache.seatunnel.tools.x2seatunnel.util.DataXFieldExtractor.FieldReferenceTracker
                    fieldTracker = dataXExtractor.createFieldReferenceTracker(sourceContent);
            variableResolver.setFieldReferenceTracker(fieldTracker);

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
            String envConfig = generateEnvConfig(dataXConfig, sourceContent);

            // 5. 验证并解析source模板
            if (!variableResolver.validateTemplate(sourceTemplateContent)) {
                throw new RuntimeException("Source模板格式错误，不符合Jinja2语法标准。请检查模板文件: " + sourceTemplate);
            }
            logger.info("使用模板分析器解析 source 模板");
            String resolvedSourceConfig =
                    variableResolver.resolveWithTemplateAnalysis(
                            sourceTemplateContent, "source", sourceContent);

            // 6. 验证并解析sink模板
            if (!variableResolver.validateTemplate(sinkTemplateContent)) {
                throw new RuntimeException("Sink模板格式错误，不符合Jinja2语法标准。请检查模板文件: " + sinkTemplate);
            }
            logger.info("使用模板分析器解析 sink 模板");
            String resolvedSinkConfig =
                    variableResolver.resolveWithTemplateAnalysis(
                            sinkTemplateContent, "sink", sourceContent);

            // 7. 组装完整的SeaTunnel配置
            String finalConfig =
                    assembleConfig(envConfig, resolvedSourceConfig, resolvedSinkConfig);

            // 8. 计算未映射字段（基于引用计数）
            mappingTracker.calculateUnmappedFieldsFromTracker(fieldTracker);

            // 9. 生成映射结果（用于报告）- 现在集成了MappingTracker数据
            MappingResult mappingResult =
                    generateMappingResult(
                            dataXConfig, readerType, writerType, sourceTemplate, sinkTemplate);

            result.setSuccess(true);
            result.setConfigContent(finalConfig);
            result.setMappingResult(mappingResult);
            result.setSourceTemplate(sourceTemplateContent); // 传递模板内容而不是路径
            result.setSinkTemplate(sinkTemplateContent); // 传递模板内容而不是路径

            logger.info("配置驱动的模板转换完成");
            logger.info("映射跟踪统计: {}", mappingTracker.getStatisticsText());

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
    private String generateEnvConfig(DataXConfig dataXConfig, String sourceContent) {
        // 根据任务类型动态选择环境模板（默认为batch）
        String jobType = "batch"; // DataX默认为批处理
        String envTemplatePath = mappingManager.getEnvTemplate(jobType);
        logger.info("为任务类型 {} 选择环境模板: {}", jobType, envTemplatePath);

        // 加载环境配置模板
        String envTemplate = loadTemplate(envTemplatePath);

        // 使用模板变量解析器处理环境配置
        String resolvedEnvConfig =
                variableResolver.resolveWithTemplateAnalysis(envTemplate, "env", sourceContent);

        return resolvedEnvConfig;
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

        // 首先从 MappingTracker 获取基础映射结果
        MappingResult result = mappingTracker.generateMappingResult();

        // 设置模板信息（这些属于基本信息，不是字段映射）
        result.setSourceTemplate(sourceTemplate);
        result.setSinkTemplate(sinkTemplate);
        result.setReaderType(readerType);
        result.setWriterType(writerType);

        // 所有配置都通过模板驱动，不在Java代码中硬编码任何配置项

        // 检查是否支持的类型
        if (!mappingManager.isReaderSupported(readerType)) {
            result.addUnmappedField("reader.name", readerType, "使用默认JDBC模板");
        }

        if (!mappingManager.isWriterSupported(writerType)) {
            result.addUnmappedField("writer.name", writerType, "使用默认HDFS模板");
        }

        result.setSuccess(true);
        logger.info(
                "生成映射结果完成，总计字段: 成功{}个, 默认值{}个, 缺失{}个, 未映射{}个",
                result.getSuccessMappings().size(),
                result.getDefaultValues().size(),
                result.getMissingRequiredFields().size(),
                result.getUnmappedFields().size());

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
