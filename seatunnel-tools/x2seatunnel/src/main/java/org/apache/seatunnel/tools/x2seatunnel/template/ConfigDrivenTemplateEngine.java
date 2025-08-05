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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.tools.x2seatunnel.model.MappingResult;
import org.apache.seatunnel.tools.x2seatunnel.model.MappingTracker;
import org.apache.seatunnel.tools.x2seatunnel.util.FileUtils;
import org.apache.seatunnel.tools.x2seatunnel.util.PathResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration-driven template conversion engine based on template-mapping.yaml configuration file
 * to automatically select and apply templates
 */
public class ConfigDrivenTemplateEngine {

    private static final Logger logger = LoggerFactory.getLogger(ConfigDrivenTemplateEngine.class);

    private final TemplateMappingManager mappingManager;
    private final TemplateVariableResolver variableResolver;
    private final MappingTracker mappingTracker; // Added: mapping tracker

    public ConfigDrivenTemplateEngine() {
        this.mappingManager = TemplateMappingManager.getInstance();
        this.mappingTracker = new MappingTracker(); // Initialize mapping tracker
        this.variableResolver =
                new TemplateVariableResolver(this.mappingManager, this.mappingTracker);
    }

    /**
     * Convert DataX configuration using configuration-driven approach
     *
     * @param sourceContent Original DataX JSON content
     * @return Conversion result
     */
    public TemplateConversionResult convertWithTemplate(String sourceContent) {
        logger.info("Starting configuration-driven template conversion...");

        TemplateConversionResult result = new TemplateConversionResult();

        try {
            // Reset mapping tracker state
            mappingTracker.reset();
            logger.info("Mapping tracker has been reset, starting new conversion process");

            // Create field reference tracker
            org.apache.seatunnel.tools.x2seatunnel.util.DataXFieldExtractor dataXExtractor =
                    new org.apache.seatunnel.tools.x2seatunnel.util.DataXFieldExtractor();
            org.apache.seatunnel.tools.x2seatunnel.util.DataXFieldExtractor.FieldReferenceTracker
                    fieldTracker = dataXExtractor.createFieldReferenceTracker(sourceContent);
            variableResolver.setFieldReferenceTracker(fieldTracker);

            // Extract reader and writer types from JSON
            String readerType = extractReaderType(sourceContent);
            String writerType = extractWriterType(sourceContent);

            // 1. Select source template based on reader type
            String sourceTemplate = mappingManager.getSourceTemplate(readerType);
            logger.info(
                    "Selected source template for reader type {}: {}", readerType, sourceTemplate);

            // 2. Select sink template based on writer type
            String sinkTemplate = mappingManager.getSinkTemplate(writerType);
            logger.info("Selected sink template for writer type {}: {}", writerType, sinkTemplate);

            // 3. Load template content
            String sourceTemplateContent = loadTemplate(sourceTemplate);
            String sinkTemplateContent = loadTemplate(sinkTemplate);

            // 4. Generate env configuration
            String envConfig = generateEnvConfig(sourceContent);

            // 5. Validate and parse source template
            if (!variableResolver.validateTemplate(sourceTemplateContent)) {
                throw new RuntimeException(
                        "Source template format error, does not conform to Jinja2 syntax standard. Please check template file: "
                                + sourceTemplate);
            }
            logger.info("Using template analyzer to parse source template");
            String resolvedSourceConfig =
                    variableResolver.resolveWithTemplateAnalysis(
                            sourceTemplateContent, "source", sourceContent);

            // 6. Validate and parse sink template
            if (!variableResolver.validateTemplate(sinkTemplateContent)) {
                throw new RuntimeException(
                        "Sink template format error, does not conform to Jinja2 syntax standard. Please check template file: "
                                + sinkTemplate);
            }
            logger.info("Using template analyzer to parse sink template");
            String resolvedSinkConfig =
                    variableResolver.resolveWithTemplateAnalysis(
                            sinkTemplateContent, "sink", sourceContent);

            // 7. Assemble complete SeaTunnel configuration
            String finalConfig =
                    assembleConfig(envConfig, resolvedSourceConfig, resolvedSinkConfig);

            // 8. Calculate unmapped fields (based on reference count)
            mappingTracker.calculateUnmappedFieldsFromTracker(fieldTracker);

            // 9. Generate mapping result (for reporting) - now integrated with MappingTracker data
            MappingResult mappingResult =
                    generateMappingResult(readerType, writerType, sourceTemplate, sinkTemplate);

            result.setSuccess(true);
            result.setConfigContent(finalConfig);
            result.setMappingResult(mappingResult);
            result.setSourceTemplate(
                    sourceTemplateContent); // Pass template content instead of path
            result.setSinkTemplate(sinkTemplateContent); // Pass template content instead of path

            logger.info("Configuration-driven template conversion completed");
            logger.info("Mapping tracking statistics: {}", mappingTracker.getStatisticsText());

        } catch (Exception e) {
            logger.error("Configuration-driven template conversion failed: {}", e.getMessage(), e);
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        }

        return result;
    }

    /** Load template file content */
    private String loadTemplate(String templatePath) {
        logger.debug("Loading template file: {}", templatePath);

        // 1. Try to load from file system
        String resolvedPath = PathResolver.resolveTemplatePath(templatePath);
        if (resolvedPath != null && PathResolver.exists(resolvedPath)) {
            logger.debug("Loading template from file system: {}", resolvedPath);
            return FileUtils.readFile(resolvedPath);
        }

        // 2. Load from classpath (built-in templates)
        try {
            String resourcePath = PathResolver.buildResourcePath(templatePath);
            logger.debug("Loading template from classpath: {}", resourcePath);
            return FileUtils.readResourceFile(resourcePath);
        } catch (Exception e) {
            throw new RuntimeException("Unable to load template file: " + templatePath, e);
        }
    }

    /** Generate environment configuration section */
    private String generateEnvConfig(String sourceContent) {
        // Dynamically select environment template based on job type (default is batch)
        String jobType = "batch"; // DataX defaults to batch processing
        String envTemplatePath = mappingManager.getEnvTemplate(jobType);
        logger.info("Selected environment template for job type {}: {}", jobType, envTemplatePath);

        // Load environment configuration template
        String envTemplate = loadTemplate(envTemplatePath);

        // Use template variable resolver to process environment configuration
        String resolvedEnvConfig =
                variableResolver.resolveWithTemplateAnalysis(envTemplate, "env", sourceContent);

        return resolvedEnvConfig;
    }

    /** Assemble complete SeaTunnel configuration */
    private String assembleConfig(String envConfig, String sourceConfig, String sinkConfig) {
        StringBuilder finalConfig = new StringBuilder();

        // Add header comments
        finalConfig.append("# SeaTunnel Configuration File\n");
        finalConfig.append("# Auto-generated by X2SeaTunnel Configuration-Driven Engine\n");
        finalConfig.append("# Generated at: ").append(java.time.LocalDateTime.now()).append("\n");
        finalConfig.append("\n");

        // Add env configuration
        finalConfig.append(envConfig).append("\n");

        // Add source configuration
        finalConfig.append(sourceConfig).append("\n");

        // Add sink configuration
        finalConfig.append(sinkConfig).append("\n");

        return finalConfig.toString();
    }

    /** Generate mapping result (for report generation) */
    private MappingResult generateMappingResult(
            String readerType, String writerType, String sourceTemplate, String sinkTemplate) {

        // First get basic mapping result from MappingTracker
        MappingResult result = mappingTracker.generateMappingResult();

        // Set template information (these are basic info, not field mappings)
        result.setSourceTemplate(sourceTemplate);
        result.setSinkTemplate(sinkTemplate);
        result.setReaderType(readerType);
        result.setWriterType(writerType);

        // All configurations are template-driven, no hardcoded configuration items in Java code

        // Check if the types are supported
        if (!mappingManager.isReaderSupported(readerType)) {
            result.addUnmappedField("reader.name", readerType, "Using default JDBC template");
        }

        if (!mappingManager.isWriterSupported(writerType)) {
            result.addUnmappedField("writer.name", writerType, "Using default HDFS template");
        }

        result.setSuccess(true);
        logger.info(
                "Mapping result generation completed, total fields: success {}, default values {}, missing {}, unmapped {}",
                result.getSuccessMappings().size(),
                result.getDefaultValues().size(),
                result.getMissingRequiredFields().size(),
                result.getUnmappedFields().size());

        return result;
    }

    /** Check if the specified configuration combination is supported */
    public boolean isConfigurationSupported(String readerType, String writerType) {
        return mappingManager.isReaderSupported(readerType)
                && mappingManager.isWriterSupported(writerType);
    }

    /** Get supported configuration information */
    public String getSupportedConfigInfo() {
        StringBuilder info = new StringBuilder();
        info.append("Supported Reader types: ");
        info.append(String.join(", ", mappingManager.getSupportedReaders()));
        info.append("\n");
        info.append("Supported Writer types: ");
        info.append(String.join(", ", mappingManager.getSupportedWriters()));
        return info.toString();
    }

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

    /**
     * Extract reader type from DataX JSON configuration
     *
     * @param sourceContent DataX JSON content
     * @return Reader type (e.g., "mysqlreader")
     */
    private String extractReaderType(String sourceContent) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(sourceContent);

            JsonNode contentNode = rootNode.path("job").path("content");
            if (contentNode.isArray() && contentNode.size() > 0) {
                JsonNode readerNode = contentNode.get(0).path("reader");
                if (readerNode.has("name")) {
                    return readerNode.get("name").asText();
                }
            }

            throw new IllegalArgumentException(
                    "Cannot extract reader type from DataX configuration");
        } catch (Exception e) {
            logger.error("Failed to extract reader type: {}", e.getMessage());
            throw new RuntimeException("Failed to extract reader type from DataX configuration", e);
        }
    }

    /**
     * Extract writer type from DataX JSON configuration
     *
     * @param sourceContent DataX JSON content
     * @return Writer type (e.g., "mysqlwriter")
     */
    private String extractWriterType(String sourceContent) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(sourceContent);

            JsonNode contentNode = rootNode.path("job").path("content");
            if (contentNode.isArray() && contentNode.size() > 0) {
                JsonNode writerNode = contentNode.get(0).path("writer");
                if (writerNode.has("name")) {
                    return writerNode.get("name").asText();
                }
            }

            throw new IllegalArgumentException(
                    "Cannot extract writer type from DataX configuration");
        } catch (Exception e) {
            logger.error("Failed to extract writer type: {}", e.getMessage());
            throw new RuntimeException("Failed to extract writer type from DataX configuration", e);
        }
    }
}
