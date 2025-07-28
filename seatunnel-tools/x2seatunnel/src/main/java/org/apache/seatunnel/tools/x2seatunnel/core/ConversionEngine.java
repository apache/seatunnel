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

package org.apache.seatunnel.tools.x2seatunnel.core;

import org.apache.seatunnel.tools.x2seatunnel.model.DataXConfig;
import org.apache.seatunnel.tools.x2seatunnel.model.MappingResult;
import org.apache.seatunnel.tools.x2seatunnel.model.MappingTracker;
import org.apache.seatunnel.tools.x2seatunnel.parser.DataXConfigParser;
import org.apache.seatunnel.tools.x2seatunnel.report.MarkdownReportGenerator;
import org.apache.seatunnel.tools.x2seatunnel.template.ConfigDrivenTemplateEngine;
import org.apache.seatunnel.tools.x2seatunnel.template.ConfigDrivenTemplateEngine.TemplateConversionResult;
import org.apache.seatunnel.tools.x2seatunnel.template.TemplateMappingManager;
import org.apache.seatunnel.tools.x2seatunnel.template.TemplateVariableResolver;
import org.apache.seatunnel.tools.x2seatunnel.util.FileUtils;
import org.apache.seatunnel.tools.x2seatunnel.util.PathResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

/** 核心转换引擎 */
public class ConversionEngine {

    private static final Logger logger = LoggerFactory.getLogger(ConversionEngine.class);

    private final TemplateVariableResolver templateResolver;
    private final ConfigDrivenTemplateEngine configDrivenEngine;
    private final TemplateMappingManager templateMappingManager;

    public ConversionEngine() {
        this.templateMappingManager = TemplateMappingManager.getInstance();
        this.templateResolver = new TemplateVariableResolver(templateMappingManager);
        this.configDrivenEngine = new ConfigDrivenTemplateEngine();
    }

    /**
     * 执行配置转换（标准转换方式）
     *
     * @param sourceFile 源文件路径
     * @param targetFile 目标文件路径
     * @param sourceType 源类型
     * @param targetType 目标类型
     * @param reportFile 报告文件路径
     */
    public void convert(
            String sourceFile,
            String targetFile,
            String sourceType,
            String targetType,
            String reportFile) {
        convert(sourceFile, targetFile, sourceType, targetType, null, reportFile);
    }

    /**
     * 执行配置转换（支持自定义模板）
     *
     * @param sourceFile 源文件路径
     * @param targetFile 目标文件路径
     * @param sourceType 源类型
     * @param targetType 目标类型
     * @param customTemplate 自定义模板文件名
     * @param reportFile 报告文件路径
     */
    public void convert(
            String sourceFile,
            String targetFile,
            String sourceType,
            String targetType,
            String customTemplate,
            String reportFile) {
        logger.info("开始执行配置转换...");
        logger.info("源文件: {}", sourceFile);
        logger.info("目标文件: {}", targetFile);
        logger.info("源类型: {}", sourceType);
        logger.info("目标类型: {}", targetType);
        if (customTemplate != null) {
            logger.info("自定义模板: {}", customTemplate);
        }

        try {
            // 读取源文件
            logger.info("正在读取输入文件...");
            String sourceContent = FileUtils.readFile(sourceFile);
            logger.info("文件读取成功，大小: {} bytes", sourceContent.length());

            // 解析DataX配置
            logger.info("正在解析{}配置...", sourceType);
            DataXConfigParser parser = new DataXConfigParser();
            DataXConfig dataXConfig = parser.parse(sourceContent);
            logger.info("配置解析完成");

            String targetContent;
            MappingResult mappingResult = null;
            TemplateConversionResult templateResult = null;

            if (customTemplate != null && !customTemplate.trim().isEmpty()) {
                // 使用自定义模板进行转换（极简方案）
                logger.info("使用自定义模板进行转换: {}", customTemplate);
                targetContent =
                        convertWithCustomTemplate(dataXConfig, customTemplate, sourceContent);
                logger.info("自定义模板转换完成");
            } else {
                // 使用配置驱动的标准转换流程
                logger.info("使用配置驱动的标准转换流程");

                // 使用配置驱动引擎进行转换
                logger.info("正在执行配置驱动的模板转换...");
                templateResult = configDrivenEngine.convertWithTemplate(dataXConfig, sourceContent);

                if (!templateResult.isSuccess()) {
                    throw new RuntimeException("配置驱动模板转换失败: " + templateResult.getErrorMessage());
                }

                targetContent = templateResult.getConfigContent();
                mappingResult = templateResult.getMappingResult();
            }

            // 生成报告（如果指定了报告文件）
            if (reportFile != null && !reportFile.trim().isEmpty()) {
                logger.info("正在生成转换报告...");
                if (mappingResult != null && templateResult != null) {
                    // 标准转换的详细报告
                    generateDetailedConversionReport(
                            mappingResult,
                            sourceFile,
                            targetFile,
                            sourceType,
                            customTemplate,
                            templateResult.getSourceTemplate(),
                            templateResult.getSinkTemplate(),
                            reportFile);
                } else {
                    // 自定义模板转换：分析自定义模板生成报告数据
                    logger.info("为自定义模板转换生成报告数据...");
                    MappingResult customMappingResult =
                            analyzeCustomTemplate(customTemplate, dataXConfig, sourceContent);
                    generateDetailedConversionReport(
                            customMappingResult,
                            sourceFile,
                            targetFile,
                            sourceType,
                            customTemplate,
                            customTemplate, // 自定义模板作为源模板
                            customTemplate, // 自定义模板作为目标模板
                            reportFile);
                }
                logger.info("转换报告生成完成: {}", reportFile);
            }

            // 写入目标文件
            logger.info("正在写入目标文件...");
            FileUtils.writeFile(targetFile, targetContent);
            logger.info("输出文件生成完成: {}", targetFile);

        } catch (Exception e) {
            logger.error("配置转换失败: {}", e.getMessage(), e);
            throw new RuntimeException("配置转换失败", e);
        }
    }

    /**
     * 使用自定义模板进行转换
     *
     * @param dataXConfig DataX配置
     * @param customTemplate 自定义模板文件名
     * @param sourceContent 原始DataX JSON内容
     * @return 转换后的配置内容
     */
    private String convertWithCustomTemplate(
            DataXConfig dataXConfig, String customTemplate, String sourceContent) {
        try {
            // 加载自定义模板
            String templateContent = loadCustomTemplate(customTemplate);

            // 使用模板变量解析器进行变量替换（使用原始JSON内容）
            return templateResolver.resolve(templateContent, sourceContent);

        } catch (Exception e) {
            logger.error("自定义模板转换失败: {}", e.getMessage(), e);
            throw new RuntimeException("自定义模板转换失败: " + e.getMessage(), e);
        }
    }

    /**
     * 加载自定义模板文件
     *
     * @param templatePath 模板文件路径（支持绝对路径和相对路径）
     * @return 模板内容
     */
    private String loadCustomTemplate(String templatePath) {
        logger.info("正在加载自定义模板: {}", templatePath);

        // 1. 使用智能路径解析器查找文件系统中的模板
        String resolvedPath = PathResolver.resolveTemplatePath(templatePath);
        if (resolvedPath != null && PathResolver.exists(resolvedPath)) {
            logger.info("从文件系统加载模板: {}", resolvedPath);
            return FileUtils.readFile(resolvedPath);
        }

        // 2. 从classpath加载（内置模板）
        try {
            String resourcePath = PathResolver.buildResourcePath(templatePath);
            logger.info("尝试从classpath加载模板: {}", resourcePath);

            String content = FileUtils.readResourceFile(resourcePath);
            if (content != null && !content.trim().isEmpty()) {
                logger.info("从classpath成功加载模板: {}", resourcePath);
                return content;
            }
        } catch (Exception e) {
            logger.debug("从classpath加载模板失败: {}", e.getMessage());
        }

        // 3. 生成详细的错误信息，帮助用户调试
        String homePath = PathResolver.getHomePath();
        String configTemplatesDir = PathResolver.getConfigTemplatesDir();

        throw new RuntimeException(
                String.format(
                        "找不到自定义模板文件: %s\n"
                                + "搜索路径:\n"
                                + "  1. 当前工作目录: %s\n"
                                + "  2. 配置模板目录: %s\n"
                                + "  3. 开发环境配置: %s/config/x2seatunnel/templates/%s\n"
                                + "  4. 内置资源: classpath:%s\n"
                                + "提示: 请检查模板文件是否存在，或使用绝对路径指定模板位置",
                        templatePath,
                        new File(templatePath).getAbsolutePath(),
                        new File(configTemplatesDir, templatePath).getAbsolutePath(),
                        homePath,
                        templatePath,
                        PathResolver.buildResourcePath(templatePath)));
    }

    /** 生成详细的转换报告 */
    private void generateDetailedConversionReport(
            MappingResult mappingResult,
            String sourceFile,
            String targetFile,
            String sourceType,
            String customTemplate,
            String sourceTemplate,
            String sinkTemplate,
            String reportFile) {
        MarkdownReportGenerator reportGenerator = new MarkdownReportGenerator();
        String reportContent =
                reportGenerator.generateReport(
                        mappingResult,
                        sourceFile,
                        targetFile,
                        sourceType,
                        customTemplate,
                        sourceTemplate,
                        sinkTemplate);
        FileUtils.writeFile(reportFile, reportContent);
    }

    /** 分析自定义模板，生成映射结果 */
    private MappingResult analyzeCustomTemplate(
            String customTemplate, DataXConfig dataXConfig, String sourceContent) {
        logger.info("开始分析自定义模板: {}", customTemplate);

        try {
            // 1. 加载自定义模板内容
            String templateContent = loadCustomTemplate(customTemplate);

            // 2. 创建专用的映射跟踪器和变量解析器
            MappingTracker customTracker = new MappingTracker();
            TemplateVariableResolver customResolver =
                    new TemplateVariableResolver(templateMappingManager, customTracker);

            // 3. 分析模板，提取字段映射关系
            logger.info("分析自定义模板的字段映射关系...");
            Map<String, List<String>> fieldMappings =
                    customResolver.analyzeTemplateFieldMappings(templateContent, "custom");
            logger.info("自定义模板包含 {} 个字段映射", fieldMappings.size());

            // 4. 解析模板变量，触发映射跟踪
            logger.info("解析自定义模板变量...");
            customResolver.resolveWithTemplateAnalysis(templateContent, "custom", sourceContent);

            // 5. 生成映射结果
            MappingResult result = customTracker.generateMappingResult();
            result.setSuccess(true);

            logger.info(
                    "自定义模板分析完成: 直接映射({})个, 转换映射({})个, 默认值({})个, 缺失({})个, 未映射({})个",
                    result.getSuccessMappings().size(),
                    result.getTransformMappings().size(),
                    result.getDefaultValues().size(),
                    result.getMissingRequiredFields().size(),
                    result.getUnmappedFields().size());

            return result;

        } catch (Exception e) {
            logger.error("自定义模板分析失败: {}", e.getMessage(), e);
            // 返回一个基本的成功结果，避免报告生成失败
            MappingResult fallbackResult = new MappingResult();
            fallbackResult.setSuccess(true);
            fallbackResult.addDefaultValueField(
                    "template.type", "custom", "使用自定义模板: " + customTemplate);
            return fallbackResult;
        }
    }
}
