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

package org.apache.seatunnel.tools.x2seatunnel.report;

import org.apache.seatunnel.tools.x2seatunnel.model.MappingResult;
import org.apache.seatunnel.tools.x2seatunnel.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/** Markdown格式转换报告生成器 */
public class MarkdownReportGenerator {
    private static final Logger logger = LoggerFactory.getLogger(MarkdownReportGenerator.class);
    private static final String TEMPLATE_PATH = "/templates/report/report-template.md";

    /**
     * 生成Markdown格式的转换报告（标准转换）
     *
     * @param result 映射结果
     * @param sourceFile 源文件路径
     * @param targetFile 目标文件路径
     * @param sourceType 源类型
     * @return Markdown报告内容
     */
    public String generateReport(
            MappingResult result, String sourceFile, String targetFile, String sourceType) {
        return generateReport(result, sourceFile, targetFile, sourceType, null, "", "");
    }

    /**
     * 生成Markdown格式的转换报告（支持自定义模板）
     *
     * @param result 映射结果
     * @param sourceFile 源文件路径
     * @param targetFile 目标文件路径
     * @param sourceType 源类型
     * @param customTemplate 自定义模板名称（可选）
     * @param sourceTemplate 源模板内容（用于提取连接器类型）
     * @param sinkTemplate 目标模板内容（用于提取连接器类型）
     * @return Markdown报告内容
     */
    public String generateReport(
            MappingResult result,
            String sourceFile,
            String targetFile,
            String sourceType,
            String customTemplate,
            String sourceTemplate,
            String sinkTemplate) {
        logger.info("生成Markdown转换报告");

        // 加载模板
        String template = loadTemplate();

        // 构建模板变量
        Map<String, String> variables =
                buildTemplateVariables(
                        result,
                        sourceFile,
                        targetFile,
                        sourceType,
                        customTemplate,
                        sourceTemplate,
                        sinkTemplate);

        // 替换模板变量
        return replaceTemplateVariables(template, variables);
    }

    /** 加载报告模板 */
    private String loadTemplate() {
        try {
            return FileUtils.readResourceFile(TEMPLATE_PATH);
        } catch (Exception e) {
            logger.warn("无法加载报告模板，使用默认格式: {}", e.getMessage());
            return getDefaultTemplate();
        }
    }

    /** 构建模板变量 */
    private Map<String, String> buildTemplateVariables(
            MappingResult result,
            String sourceFile,
            String targetFile,
            String sourceType,
            String customTemplate,
            String sourceTemplate,
            String sinkTemplate) {

        Map<String, String> variables = new HashMap<>();

        // 基本信息
        variables.put("convertTime", LocalDateTime.now().toString());
        variables.put("sourceFile", formatFilePath(sourceFile));
        variables.put("targetFile", formatFilePath(targetFile));
        variables.put("sourceType", sourceType.toUpperCase());
        variables.put("sourceTypeName", sourceType.toUpperCase());
        variables.put("status", result.isSuccess() ? "✅ 成功" : "❌ 失败");
        variables.put("generateTime", LocalDateTime.now().toString());

        // 连接器类型识别
        variables.put("sourceConnector", extractConnectorType(sourceTemplate, "Jdbc", result));
        variables.put("sinkConnector", extractConnectorType(sinkTemplate, "HdfsFile", result));

        // 自定义模板信息
        if (customTemplate != null && !customTemplate.trim().isEmpty()) {
            variables.put("customTemplateInfo", "| **自定义模板** | `" + customTemplate + "` |");
        } else {
            variables.put("customTemplateInfo", "");
        }

        // 错误信息
        if (!result.isSuccess() && result.getErrorMessage() != null) {
            variables.put(
                    "errorInfo", "### ⚠️ 错误信息\n\n```\n" + result.getErrorMessage() + "\n```\n");
        } else {
            variables.put("errorInfo", "");
        }

        // 统计信息
        buildStatistics(variables, result);

        // 各种表格
        variables.put("directMappingTable", buildDirectMappingTable(result, sourceType));
        variables.put("transformMappingTable", buildTransformMappingTable(result, sourceType));
        variables.put("defaultValuesTable", buildDefaultValuesTable(result));
        variables.put("missingFieldsTable", buildMissingFieldsTable(result));
        variables.put("unmappedFieldsTable", buildUnmappedFieldsTable(result));

        return variables;
    }

    /** 构建统计信息 */
    private void buildStatistics(Map<String, String> variables, MappingResult result) {
        int directCount = result.getSuccessMappings().size();
        int transformCount = result.getTransformMappings().size();
        int defaultCount = result.getDefaultValues().size();
        int missingCount = result.getMissingRequiredFields().size();
        int unmappedCount = result.getUnmappedFields().size();
        int totalCount = directCount + transformCount + defaultCount + missingCount + unmappedCount;

        variables.put("directCount", String.valueOf(directCount));
        variables.put("transformCount", String.valueOf(transformCount));
        variables.put("defaultCount", String.valueOf(defaultCount));
        variables.put("missingCount", String.valueOf(missingCount));
        variables.put("unmappedCount", String.valueOf(unmappedCount));
        variables.put("totalCount", String.valueOf(totalCount));

        if (totalCount > 0) {
            variables.put(
                    "directPercent",
                    String.format("%.1f%%", (double) directCount / totalCount * 100));
            variables.put(
                    "transformPercent",
                    String.format("%.1f%%", (double) transformCount / totalCount * 100));
            variables.put(
                    "defaultPercent",
                    String.format("%.1f%%", (double) defaultCount / totalCount * 100));
            variables.put(
                    "missingPercent",
                    String.format("%.1f%%", (double) missingCount / totalCount * 100));
            variables.put(
                    "unmappedPercent",
                    String.format("%.1f%%", (double) unmappedCount / totalCount * 100));
        } else {
            variables.put("successPercent", "0%");
            variables.put("autoPercent", "0%");
            variables.put("defaultPercent", "0%"); // 新增：默认值百分比
            variables.put("missingPercent", "0%");
            variables.put("unmappedPercent", "0%");
        }
    }

    /** 构建成功映射表格 */
    /** 构建直接映射字段表格 */
    private String buildDirectMappingTable(MappingResult result, String sourceType) {
        if (result.getSuccessMappings().isEmpty()) {
            return "*无直接映射的字段*\n";
        }

        StringBuilder table = new StringBuilder();
        table.append("| SeaTunnel字段 | 值 | ").append(sourceType.toUpperCase()).append("来源字段 |\n");
        table.append("|---------------|----|--------------|\n");

        for (MappingResult.MappingItem item : result.getSuccessMappings()) {
            table.append("| `")
                    .append(item.getTargetField())
                    .append("` | `")
                    .append(item.getValue())
                    .append("` | `")
                    .append(item.getSourceField())
                    .append("` |\n");
        }

        return table.toString();
    }

    /** 构建转换映射字段表格 */
    private String buildTransformMappingTable(MappingResult result, String sourceType) {
        if (result.getTransformMappings().isEmpty()) {
            return "*无转换映射的字段*\n";
        }

        StringBuilder table = new StringBuilder();
        table.append("| SeaTunnel字段 | 值 | ")
                .append(sourceType.toUpperCase())
                .append("来源字段 | 使用过滤器 |\n");
        table.append("|---------------|----|--------------|-----------|\n");

        for (MappingResult.TransformMapping item : result.getTransformMappings()) {
            table.append("| `")
                    .append(item.getTargetField())
                    .append("` | `")
                    .append(item.getValue())
                    .append("` | `")
                    .append(item.getSourceField())
                    .append("` | ")
                    .append(item.getFilterName())
                    .append(" |\n");
        }

        return table.toString();
    }

    /** 构建默认值字段表格 */
    private String buildDefaultValuesTable(MappingResult result) {
        if (result.getDefaultValues().isEmpty()) {
            return "*无使用默认值的字段*\n";
        }

        StringBuilder table = new StringBuilder();
        table.append("| SeaTunnel字段 | 默认值 |\n");
        table.append("|---------------|--------|\n");

        for (MappingResult.DefaultValueField field : result.getDefaultValues()) {
            table.append("| `")
                    .append(field.getFieldName())
                    .append("` | `")
                    .append(field.getValue())
                    .append("` |\n");
        }

        return table.toString();
    }

    /** 构建缺失字段表格 */
    private String buildMissingFieldsTable(MappingResult result) {
        if (result.getMissingRequiredFields().isEmpty()) {
            return "*无缺失的字段* 🎉\n";
        }

        StringBuilder table = new StringBuilder();
        table.append("⚠️ **注意**: 以下字段在源配置中未找到，请手动补充：\n\n");
        table.append("| SeaTunnel字段 |\n");
        table.append("|---------------|\n");

        for (MappingResult.MissingField field : result.getMissingRequiredFields()) {
            table.append("| `").append(field.getFieldName()).append("` |\n");
        }

        return table.toString();
    }

    /** 构建未映射字段表格 */
    private String buildUnmappedFieldsTable(MappingResult result) {
        if (result.getUnmappedFields().isEmpty()) {
            return "*所有字段都已映射* 🎉\n";
        }

        StringBuilder table = new StringBuilder();
        table.append("| DataX字段 | 值 |\n");
        table.append("|--------|------|\n");

        for (MappingResult.UnmappedField field : result.getUnmappedFields()) {
            table.append("| `")
                    .append(field.getFieldName())
                    .append("` | `")
                    .append(field.getValue())
                    .append("` |\n");
        }

        return table.toString();
    }

    /** 从模板内容中提取连接器类型 */
    private String extractConnectorType(
            String templateContent, String defaultType, MappingResult result) {
        if (templateContent == null || templateContent.trim().isEmpty()) {
            logger.warn("模板内容为空，使用默认类型: {}", defaultType);
            return defaultType;
        }

        logger.debug("正在分析模板内容提取连接器类型，模板长度: {}", templateContent.length());
        logger.debug(
                "模板内容前200字符: {}",
                templateContent.substring(0, Math.min(200, templateContent.length())));

        // 查找模板中的连接器类型（如 Jdbc {, HdfsFile {, Kafka { 等）
        // 需要跳过顶层的 source { 和 sink {，查找嵌套的连接器类型
        String[] lines = templateContent.split("\n");
        boolean inSourceOrSink = false;

        for (String line : lines) {
            String trimmed = line.trim();

            // 检测是否进入 source { 或 sink { 块
            if (trimmed.equals("source {") || trimmed.equals("sink {")) {
                inSourceOrSink = true;
                continue;
            }

            // 在 source/sink 块内查找连接器类型
            if (inSourceOrSink && trimmed.matches("\\w+\\s*\\{")) {
                String connectorType = trimmed.substring(0, trimmed.indexOf('{')).trim();
                logger.info("找到连接器类型: {}", connectorType);

                // 添加数据库类型识别（对于JDBC连接器）
                if ("Jdbc".equals(connectorType)) {
                    String dbType = extractDatabaseTypeFromMappingResult(result);
                    if (dbType != null) {
                        logger.info("识别到数据库类型: {}", dbType);
                        return connectorType + " (" + dbType + ")";
                    }
                }
                return connectorType;
            }

            // 检测是否退出 source/sink 块（遇到顶层的 }）
            if (inSourceOrSink && trimmed.equals("}") && !line.startsWith("  ")) {
                inSourceOrSink = false;
            }
        }

        logger.warn("未找到连接器类型，使用默认类型: {}", defaultType);
        return defaultType;
    }

    /** 从映射结果中提取数据库类型 */
    private String extractDatabaseTypeFromMappingResult(MappingResult result) {
        if (result == null) {
            return null;
        }

        // 从成功映射中查找JDBC URL
        for (MappingResult.MappingItem mapping : result.getSuccessMappings()) {
            String targetField = mapping.getTargetField();
            String value = mapping.getValue();

            // 查找包含 .url 的字段，且值是JDBC URL
            if (targetField != null
                    && targetField.contains(".url")
                    && value != null
                    && value.startsWith("jdbc:")) {
                String dbType = extractDatabaseTypeFromUrl(value);
                if (dbType != null) {
                    logger.debug("从映射结果中识别数据库类型: {} -> {}", value, dbType);
                    return dbType;
                }
            }
        }

        logger.debug("映射结果中未找到JDBC URL");
        return null;
    }

    /** 从JDBC URL中提取数据库类型（使用正则表达式） */
    private String extractDatabaseTypeFromUrl(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
            return null;
        }

        try {
            // 使用正则表达式从 "jdbc:mysql://..." 中提取 "mysql"
            if (jdbcUrl.startsWith("jdbc:")) {
                String dbType = jdbcUrl.replaceFirst("^jdbc:([^:]+):.*", "$1");
                if (!dbType.equals(jdbcUrl)) { // 确保正则匹配成功
                    logger.debug("通过正则表达式识别数据库类型: {} -> {}", jdbcUrl, dbType);
                    return dbType;
                }
            }
        } catch (Exception e) {
            logger.warn("正则提取数据库类型失败: {}", e.getMessage());
        }

        logger.debug("无法从URL识别数据库类型: {}", jdbcUrl);
        return null;
    }

    /** 替换模板变量 */
    private String replaceTemplateVariables(String template, Map<String, String> variables) {
        String result = template;
        for (Map.Entry<String, String> entry : variables.entrySet()) {
            String placeholder = "{{" + entry.getKey() + "}}";
            result = result.replace(placeholder, entry.getValue());
        }
        return result;
    }

    /** 获取默认模板（当模板文件无法加载时使用） */
    private String getDefaultTemplate() {
        return "# X2SeaTunnel 转换报告\n\n"
                + "## 📋 基本信息\n\n"
                + "- **转换时间**: {{convertTime}}\n"
                + "- **源文件**: {{sourceFile}}\n"
                + "- **目标文件**: {{targetFile}}\n"
                + "- **转换状态**: {{status}}\n\n"
                + "转换完成！";
    }

    /** 格式化文件路径，将绝对路径转换为相对路径（基于当前工作目录） */
    private String formatFilePath(String filePath) {
        if (filePath == null) {
            return "";
        }

        try {
            // 获取当前工作目录
            String currentDir = System.getProperty("user.dir");

            // 如果是绝对路径且在当前工作目录下，转换为相对路径
            if (filePath.startsWith(currentDir)) {
                String relativePath = filePath.substring(currentDir.length());
                // 去掉开头的分隔符
                if (relativePath.startsWith("\\") || relativePath.startsWith("/")) {
                    relativePath = relativePath.substring(1);
                }
                return relativePath.replace("\\", "/"); // 统一使用正斜杠
            }

            // 否则返回原路径
            return filePath.replace("\\", "/"); // 统一使用正斜杠
        } catch (Exception e) {
            logger.warn("格式化文件路径失败: {}", e.getMessage());
            return filePath;
        }
    }
}
