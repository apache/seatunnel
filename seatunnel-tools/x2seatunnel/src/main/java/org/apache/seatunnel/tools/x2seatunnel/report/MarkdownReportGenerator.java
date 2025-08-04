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

/** Markdown format conversion report generator */
public class MarkdownReportGenerator {
    private static final Logger logger = LoggerFactory.getLogger(MarkdownReportGenerator.class);
    private static final String TEMPLATE_PATH = "/templates/report/report-template.md";

    /**
     * Generate Markdown format conversion report (standard conversion)
     *
     * @param result mapping result
     * @param sourceFile source file path
     * @param targetFile target file path
     * @param sourceType source type
     * @return Markdown report content
     */
    public String generateReport(
            MappingResult result, String sourceFile, String targetFile, String sourceType) {
        return generateReport(result, sourceFile, targetFile, sourceType, null, "", "");
    }

    /**
     * Generate Markdown format conversion report (supports custom templates)
     *
     * @param result mapping result
     * @param sourceFile source file path
     * @param targetFile target file path
     * @param sourceType source type
     * @param customTemplate custom template name (optional)
     * @param sourceTemplate source template content (for extracting connector type)
     * @param sinkTemplate sink template content (for extracting connector type)
     * @return Markdown report content
     */
    public String generateReport(
            MappingResult result,
            String sourceFile,
            String targetFile,
            String sourceType,
            String customTemplate,
            String sourceTemplate,
            String sinkTemplate) {
        logger.info("Generating Markdown conversion report");

        // Load template
        String template = loadTemplate();

        // Build template variables
        Map<String, String> variables =
                buildTemplateVariables(
                        result,
                        sourceFile,
                        targetFile,
                        sourceType,
                        customTemplate,
                        sourceTemplate,
                        sinkTemplate);

        // Replace template variables
        return replaceTemplateVariables(template, variables);
    }

    /** Load report template */
    private String loadTemplate() {
        try {
            return FileUtils.readResourceFile(TEMPLATE_PATH);
        } catch (Exception e) {
            logger.warn("Unable to load report template, using default format: {}", e.getMessage());
            return getDefaultTemplate();
        }
    }

    /** Build template variables */
    private Map<String, String> buildTemplateVariables(
            MappingResult result,
            String sourceFile,
            String targetFile,
            String sourceType,
            String customTemplate,
            String sourceTemplate,
            String sinkTemplate) {

        Map<String, String> variables = new HashMap<>();

        // Basic information
        variables.put("convertTime", LocalDateTime.now().toString());
        variables.put("sourceFile", formatFilePath(sourceFile));
        variables.put("targetFile", formatFilePath(targetFile));
        variables.put("sourceType", sourceType.toUpperCase());
        variables.put("sourceTypeName", sourceType.toUpperCase());
        variables.put("status", result.isSuccess() ? "✅ Success" : "❌ Failed");
        variables.put("generateTime", LocalDateTime.now().toString());

        // Connector type identification
        variables.put("sourceConnector", extractConnectorType(sourceTemplate, "Jdbc", result));
        variables.put("sinkConnector", extractConnectorType(sinkTemplate, "HdfsFile", result));

        // Custom template information
        if (customTemplate != null && !customTemplate.trim().isEmpty()) {
            variables.put(
                    "customTemplateInfo", "| **Custom Template** | `" + customTemplate + "` |");
        } else {
            variables.put("customTemplateInfo", "");
        }

        // Error information
        if (!result.isSuccess() && result.getErrorMessage() != null) {
            variables.put(
                    "errorInfo",
                    "### ⚠️ Error Information\n\n```\n" + result.getErrorMessage() + "\n```\n");
        } else {
            variables.put("errorInfo", "");
        }

        // Statistics information
        buildStatistics(variables, result);

        // Various tables
        variables.put("directMappingTable", buildDirectMappingTable(result, sourceType));
        variables.put("transformMappingTable", buildTransformMappingTable(result, sourceType));
        variables.put("defaultValuesTable", buildDefaultValuesTable(result));
        variables.put("missingFieldsTable", buildMissingFieldsTable(result));
        variables.put("unmappedFieldsTable", buildUnmappedFieldsTable(result));

        return variables;
    }

    /** Build statistics information */
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
            variables.put("defaultPercent", "0%");
            variables.put("missingPercent", "0%");
            variables.put("unmappedPercent", "0%");
        }
    }

    /** Build direct mapping fields table */
    private String buildDirectMappingTable(MappingResult result, String sourceType) {
        if (result.getSuccessMappings().isEmpty()) {
            return "*No direct mapped fields*\n";
        }

        StringBuilder table = new StringBuilder();
        table.append("| SeaTunnel Field | Value | ")
                .append(sourceType.toUpperCase())
                .append(" Source Field |\n");
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

    /** Build transform mapping fields table */
    private String buildTransformMappingTable(MappingResult result, String sourceType) {
        if (result.getTransformMappings().isEmpty()) {
            return "*No transform mapped fields*\n";
        }

        StringBuilder table = new StringBuilder();
        table.append("| SeaTunnel Field | Value | ")
                .append(sourceType.toUpperCase())
                .append(" Source Field | Filter Used |\n");
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

    /** Build default value fields table */
    private String buildDefaultValuesTable(MappingResult result) {
        if (result.getDefaultValues().isEmpty()) {
            return "*No fields using default values*\n";
        }

        StringBuilder table = new StringBuilder();
        table.append("| SeaTunnel Field | Default Value |\n");
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

    /** Build missing fields table */
    private String buildMissingFieldsTable(MappingResult result) {
        if (result.getMissingRequiredFields().isEmpty()) {
            return "*No missing fields* 🎉\n";
        }

        StringBuilder table = new StringBuilder();
        table.append(
                "⚠️ **Note**: The following fields were not found in the source configuration, please add manually:\n\n");
        table.append("| SeaTunnel Field |\n");
        table.append("|---------------|\n");

        for (MappingResult.MissingField field : result.getMissingRequiredFields()) {
            table.append("| `").append(field.getFieldName()).append("` |\n");
        }

        return table.toString();
    }

    /** Build unmapped fields table */
    private String buildUnmappedFieldsTable(MappingResult result) {
        if (result.getUnmappedFields().isEmpty()) {
            return "*All fields are mapped* 🎉\n";
        }

        StringBuilder table = new StringBuilder();
        table.append("| DataX Field | Value |\n");
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

    /** Extract connector type from template content */
    private String extractConnectorType(
            String templateContent, String defaultType, MappingResult result) {
        if (templateContent == null || templateContent.trim().isEmpty()) {
            logger.warn("Template content is empty, using default type: {}", defaultType);
            return defaultType;
        }

        logger.debug(
                "Analyzing template content to extract connector type, template length: {}",
                templateContent.length());
        logger.debug(
                "Template content first 200 characters: {}",
                templateContent.substring(0, Math.min(200, templateContent.length())));

        // Find connector type in template (e.g. Jdbc {, HdfsFile {, Kafka {, etc.)
        // Need to skip top-level source { and sink {, look for nested connector types
        String[] lines = templateContent.split("\n");
        boolean inSourceOrSink = false;

        for (String line : lines) {
            String trimmed = line.trim();

            // Detect if entering source { or sink { block
            if (trimmed.equals("source {") || trimmed.equals("sink {")) {
                inSourceOrSink = true;
                continue;
            }

            // Look for connector type within source/sink block
            if (inSourceOrSink && trimmed.matches("\\w+\\s*\\{")) {
                String connectorType = trimmed.substring(0, trimmed.indexOf('{')).trim();
                logger.info("Found connector type: {}", connectorType);

                // Add database type identification (for JDBC connector)
                if ("Jdbc".equals(connectorType)) {
                    String dbType = extractDatabaseTypeFromMappingResult(result);
                    if (dbType != null) {
                        logger.info("Identified database type: {}", dbType);
                        return connectorType + " (" + dbType + ")";
                    }
                }
                return connectorType;
            }

            // Detect if exiting source/sink block (encountering top-level })
            if (inSourceOrSink && trimmed.equals("}") && !line.startsWith("  ")) {
                inSourceOrSink = false;
            }
        }

        logger.warn("Connector type not found, using default type: {}", defaultType);
        return defaultType;
    }

    /** Extract database type from mapping result */
    private String extractDatabaseTypeFromMappingResult(MappingResult result) {
        if (result == null) {
            return null;
        }

        // Look for JDBC URL in successful mappings
        for (MappingResult.MappingItem mapping : result.getSuccessMappings()) {
            String targetField = mapping.getTargetField();
            String value = mapping.getValue();

            // Look for fields containing .url with JDBC URL value
            if (targetField != null
                    && targetField.contains(".url")
                    && value != null
                    && value.startsWith("jdbc:")) {
                String dbType = extractDatabaseTypeFromUrl(value);
                if (dbType != null) {
                    logger.debug(
                            "Identified database type from mapping result: {} -> {}",
                            value,
                            dbType);
                    return dbType;
                }
            }
        }

        logger.debug("JDBC URL not found in mapping result");
        return null;
    }

    /** Extract database type from JDBC URL (using regular expression) */
    private String extractDatabaseTypeFromUrl(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
            return null;
        }

        try {
            // Use regular expression to extract "mysql" from "jdbc:mysql://..."
            if (jdbcUrl.startsWith("jdbc:")) {
                String dbType = jdbcUrl.replaceFirst("^jdbc:([^:]+):.*", "$1");
                if (!dbType.equals(jdbcUrl)) { // Ensure regex match succeeded
                    logger.debug("Identified database type via regex: {} -> {}", jdbcUrl, dbType);
                    return dbType;
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to extract database type via regex: {}", e.getMessage());
        }

        logger.debug("Unable to identify database type from URL: {}", jdbcUrl);
        return null;
    }

    /** Replace template variables */
    private String replaceTemplateVariables(String template, Map<String, String> variables) {
        String result = template;
        for (Map.Entry<String, String> entry : variables.entrySet()) {
            String placeholder = "{{" + entry.getKey() + "}}";
            result = result.replace(placeholder, entry.getValue());
        }
        return result;
    }

    /** Get default template (used when template file cannot be loaded) */
    private String getDefaultTemplate() {
        return "# X2SeaTunnel Conversion Report\n\n"
                + "## 📋 Basic Information\n\n"
                + "- **Conversion Time**: {{convertTime}}\n"
                + "- **Source File**: {{sourceFile}}\n"
                + "- **Target File**: {{targetFile}}\n"
                + "- **Conversion Status**: {{status}}\n\n"
                + "Conversion completed!";
    }

    /**
     * Format file path, convert absolute path to relative path (based on current working directory)
     */
    private String formatFilePath(String filePath) {
        if (filePath == null) {
            return "";
        }

        try {
            // Get current working directory
            String currentDir = System.getProperty("user.dir");

            // If it's an absolute path under current working directory, convert to relative path
            if (filePath.startsWith(currentDir)) {
                String relativePath = filePath.substring(currentDir.length());
                // Remove leading separator
                if (relativePath.startsWith("\\") || relativePath.startsWith("/")) {
                    relativePath = relativePath.substring(1);
                }
                return relativePath.replace("\\", "/"); // Use forward slash uniformly
            }

            // Otherwise return original path
            return filePath.replace("\\", "/"); // Use forward slash uniformly
        } catch (Exception e) {
            logger.warn("Failed to format file path: {}", e.getMessage());
            return filePath;
        }
    }
}
