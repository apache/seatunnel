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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.tools.x2seatunnel.model.MappingResult;
import org.apache.seatunnel.tools.x2seatunnel.model.MappingTracker;
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

/** Core conversion engine */
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
     * Execute configuration conversion (standard conversion method)
     *
     * @param sourceFile Source file path
     * @param targetFile Target file path
     * @param sourceType Source type
     * @param targetType Target type
     * @param reportFile Report file path
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
     * Execute configuration conversion (supports custom templates)
     *
     * @param sourceFile Source file path
     * @param targetFile Target file path
     * @param sourceType Source type
     * @param targetType Target type
     * @param customTemplate Custom template file name
     * @param reportFile Report file path
     */
    public void convert(
            String sourceFile,
            String targetFile,
            String sourceType,
            String targetType,
            String customTemplate,
            String reportFile) {
        logger.info("Starting configuration conversion...");
        logger.info("Source file: {}", sourceFile);
        logger.info("Target file: {}", targetFile);
        logger.info("Source type: {}", sourceType);
        logger.info("Target type: {}", targetType);
        if (customTemplate != null) {
            logger.info("Custom template: {}", customTemplate);
        }

        try {
            // Read source file
            logger.info("Reading input file...");
            String sourceContent = FileUtils.readFile(sourceFile);
            logger.info("File read successfully, size: {} bytes", sourceContent.length());

            // Validate DataX configuration format
            logger.info("Validating {} configuration format...", sourceType);
            validateDataXFormat(sourceContent);
            logger.info("Configuration validation completed");

            String targetContent;
            MappingResult mappingResult = null;
            TemplateConversionResult templateResult = null;

            if (customTemplate != null && !customTemplate.trim().isEmpty()) {
                // Use custom template for conversion (simplified approach)
                logger.info("Using custom template for conversion: {}", customTemplate);
                targetContent = convertWithCustomTemplate(customTemplate, sourceContent);
                logger.info("Custom template conversion completed");
            } else {
                // Use configuration-driven standard conversion process
                logger.info("Using configuration-driven standard conversion process");

                templateResult = configDrivenEngine.convertWithTemplate(sourceContent);

                if (!templateResult.isSuccess()) {
                    throw new RuntimeException(
                            "Configuration-driven template conversion failed: "
                                    + templateResult.getErrorMessage());
                }

                targetContent = templateResult.getConfigContent();
                mappingResult = templateResult.getMappingResult();
            }

            // Generate report (if report file is specified)
            if (reportFile != null && !reportFile.trim().isEmpty()) {
                logger.info("Generating conversion report...");
                if (mappingResult != null && templateResult != null) {
                    // Detailed report for standard conversion
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
                    // Custom template conversion: analyze custom template to generate report data
                    logger.info("Generating report data for custom template conversion...");
                    MappingResult customMappingResult =
                            analyzeCustomTemplate(customTemplate, sourceContent);
                    generateDetailedConversionReport(
                            customMappingResult,
                            sourceFile,
                            targetFile,
                            sourceType,
                            customTemplate,
                            customTemplate, // Custom template as source template
                            customTemplate, // Custom template as target template
                            reportFile);
                }
                logger.info("Conversion report generation completed: {}", reportFile);
            }

            // Write target file
            logger.info("Writing target file...");
            FileUtils.writeFile(targetFile, targetContent);
            logger.info("Output file generation completed: {}", targetFile);

        } catch (Exception e) {
            logger.error("Configuration conversion failed: {}", e.getMessage(), e);
            throw new RuntimeException("Configuration conversion failed", e);
        }
    }

    /**
     * Convert using custom template
     *
     * @param customTemplate Custom template file name
     * @param sourceContent Original DataX JSON content
     * @return Converted configuration content
     */
    private String convertWithCustomTemplate(String customTemplate, String sourceContent) {
        try {
            // Load custom template
            String templateContent = loadCustomTemplate(customTemplate);

            // Use template variable resolver for variable substitution (using original JSON
            // content)
            return templateResolver.resolve(templateContent, sourceContent);

        } catch (Exception e) {
            logger.error("Custom template conversion failed: {}", e.getMessage(), e);
            throw new RuntimeException("Custom template conversion failed: " + e.getMessage(), e);
        }
    }

    /**
     * Load custom template file
     *
     * @param templatePath Template file path (supports absolute and relative paths)
     * @return Template content
     */
    private String loadCustomTemplate(String templatePath) {
        logger.info("Loading custom template: {}", templatePath);

        // 1. Use intelligent path resolver to find template in file system
        String resolvedPath = PathResolver.resolveTemplatePath(templatePath);
        if (resolvedPath != null && PathResolver.exists(resolvedPath)) {
            logger.info("Loading template from file system: {}", resolvedPath);
            return FileUtils.readFile(resolvedPath);
        }

        // 2. Load from classpath (built-in templates)
        try {
            String resourcePath = PathResolver.buildResourcePath(templatePath);
            logger.info("Attempting to load template from classpath: {}", resourcePath);

            String content = FileUtils.readResourceFile(resourcePath);
            if (content != null && !content.trim().isEmpty()) {
                logger.info("Successfully loaded template from classpath: {}", resourcePath);
                return content;
            }
        } catch (Exception e) {
            logger.debug("Failed to load template from classpath: {}", e.getMessage());
        }

        // 3. Generate detailed error information to help users debug
        String homePath = PathResolver.getHomePath();
        String configTemplatesDir = PathResolver.getConfigTemplatesDir();

        throw new RuntimeException(
                String.format(
                        "Custom template file not found: %s\n"
                                + "Search paths:\n"
                                + "  1. Current working directory: %s\n"
                                + "  2. Configuration template directory: %s\n"
                                + "  3. Development environment configuration: %s/config/x2seatunnel/templates/%s\n"
                                + "  4. Built-in resources: classpath:%s\n"
                                + "Hint: Please check if the template file exists, or use absolute path to specify template location",
                        templatePath,
                        new File(templatePath).getAbsolutePath(),
                        new File(configTemplatesDir, templatePath).getAbsolutePath(),
                        homePath,
                        templatePath,
                        PathResolver.buildResourcePath(templatePath)));
    }

    /** Generate detailed conversion report */
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

    /**
     * Validate DataX configuration format
     *
     * @param sourceContent DataX JSON content
     * @throws IllegalArgumentException if configuration format is invalid
     */
    private void validateDataXFormat(String sourceContent) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(sourceContent);

            // Validate basic structure
            if (!rootNode.has("job")) {
                throw new IllegalArgumentException(
                        "DataX configuration missing required 'job' node");
            }

            JsonNode jobNode = rootNode.get("job");
            if (!jobNode.has("content")) {
                throw new IllegalArgumentException(
                        "DataX configuration missing required 'content' node");
            }

            JsonNode contentNode = jobNode.get("content");
            if (!contentNode.isArray() || contentNode.size() == 0) {
                throw new IllegalArgumentException(
                        "DataX configuration 'content' must be a non-empty array");
            }

            // Validate first content item has reader and writer
            JsonNode firstContent = contentNode.get(0);
            if (!firstContent.has("reader")) {
                throw new IllegalArgumentException(
                        "DataX configuration missing required 'reader' configuration");
            }
            if (!firstContent.has("writer")) {
                throw new IllegalArgumentException(
                        "DataX configuration missing required 'writer' configuration");
            }

        } catch (Exception e) {
            logger.error("DataX configuration validation failed: {}", e.getMessage());
            throw new IllegalArgumentException(
                    "Invalid DataX configuration format: " + e.getMessage(), e);
        }
    }

    /** Analyze custom template and generate mapping result */
    private MappingResult analyzeCustomTemplate(String customTemplate, String sourceContent) {
        logger.info("Starting analysis of custom template: {}", customTemplate);

        try {
            // 1. Load custom template content
            String templateContent = loadCustomTemplate(customTemplate);

            // 2. Create dedicated mapping tracker and variable resolver
            MappingTracker customTracker = new MappingTracker();
            TemplateVariableResolver customResolver =
                    new TemplateVariableResolver(templateMappingManager, customTracker);

            // 3. Analyze template and extract field mapping relationships
            logger.info("Analyzing field mapping relationships in custom template...");
            Map<String, List<String>> fieldMappings =
                    customResolver.analyzeTemplateFieldMappings(templateContent, "custom");
            logger.info("Custom template contains {} field mappings", fieldMappings.size());

            // 4. Parse template variables and trigger mapping tracking
            logger.info("Parsing custom template variables...");
            customResolver.resolveWithTemplateAnalysis(templateContent, "custom", sourceContent);

            // 5. Generate mapping result
            MappingResult result = customTracker.generateMappingResult();
            result.setSuccess(true);

            logger.info(
                    "Custom template analysis completed: direct mappings({}), transform mappings({}), default values({}), missing({}), unmapped({})",
                    result.getSuccessMappings().size(),
                    result.getTransformMappings().size(),
                    result.getDefaultValues().size(),
                    result.getMissingRequiredFields().size(),
                    result.getUnmappedFields().size());

            return result;

        } catch (Exception e) {
            logger.error("Custom template analysis failed: {}", e.getMessage(), e);
            // Return a basic success result to avoid report generation failure
            MappingResult fallbackResult = new MappingResult();
            fallbackResult.setSuccess(true);
            fallbackResult.addDefaultValueField(
                    "template.type", "custom", "Using custom template: " + customTemplate);
            return fallbackResult;
        }
    }
}
