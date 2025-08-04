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

import org.apache.seatunnel.tools.x2seatunnel.util.FileUtils;
import org.apache.seatunnel.tools.x2seatunnel.util.PathResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.util.HashMap;
import java.util.Map;

/**
 * Template mapping configuration manager responsible for loading and managing template-mapping.yaml
 * configuration file
 */
public class TemplateMappingManager {

    private static final Logger logger = LoggerFactory.getLogger(TemplateMappingManager.class);

    private static final String TEMPLATE_MAPPING_CONFIG = "template-mapping.yaml";

    private static TemplateMappingManager instance;

    private Map<String, Object> mappingConfig;
    private Map<String, String> sourceMappings;
    private Map<String, String> sinkMappings;
    private Map<String, String> envMappings;
    private Map<String, Object> transformers;

    private TemplateMappingManager() {
        loadMappingConfig();
    }

    public static synchronized TemplateMappingManager getInstance() {
        if (instance == null) {
            instance = new TemplateMappingManager();
        }
        return instance;
    }

    /** Load template mapping configuration */
    @SuppressWarnings("unchecked")
    private void loadMappingConfig() {
        logger.info("Loading template mapping configuration...");

        try {
            // 1. Try to load from file system
            String configPath = PathResolver.resolveTemplatePath(TEMPLATE_MAPPING_CONFIG);
            if (configPath != null && PathResolver.exists(configPath)) {
                logger.info(
                        "Loading template mapping configuration from file system: {}", configPath);
                String content = FileUtils.readFile(configPath);
                parseMappingConfig(content);
                return;
            }

            // 2. Load from classpath (built-in configuration)
            String resourcePath = "templates/" + TEMPLATE_MAPPING_CONFIG;
            logger.info("Loading template mapping configuration from classpath: {}", resourcePath);
            String content = FileUtils.readResourceFile(resourcePath);
            parseMappingConfig(content);

        } catch (Exception e) {
            logger.error("Failed to load template mapping configuration: {}", e.getMessage(), e);
            // Use default configuration
            initDefaultMappings();
        }
    }

    /** Parse mapping configuration content */
    @SuppressWarnings("unchecked")
    private void parseMappingConfig(String content) {
        Yaml yaml = new Yaml();
        mappingConfig = yaml.load(content);

        if (mappingConfig != null && mappingConfig.containsKey("datax")) {
            Map<String, Object> dataxConfig = (Map<String, Object>) mappingConfig.get("datax");

            // Load source mappings
            if (dataxConfig.containsKey("source_mappings")) {
                sourceMappings = (Map<String, String>) dataxConfig.get("source_mappings");
                logger.info("Loaded {} source mappings", sourceMappings.size());
            }

            // Load sink mappings
            if (dataxConfig.containsKey("sink_mappings")) {
                sinkMappings = (Map<String, String>) dataxConfig.get("sink_mappings");
                logger.info("Loaded {} sink mappings", sinkMappings.size());
            }

            // Load environment mappings
            if (dataxConfig.containsKey("env_mappings")) {
                envMappings = (Map<String, String>) dataxConfig.get("env_mappings");
                logger.info("Loaded {} environment mappings", envMappings.size());
            }
        }

        // Load transformer configuration
        if (mappingConfig != null && mappingConfig.containsKey("transformers")) {
            transformers = (Map<String, Object>) mappingConfig.get("transformers");
            logger.info("Loaded {} transformers", transformers.size());
        }

        logger.info("Template mapping configuration loading completed");
    }

    /** Initialize default mappings (fallback) - use built-in configuration file */
    private void initDefaultMappings() {
        logger.warn("Using built-in default template mapping configuration");

        try {
            // Try to load default configuration from built-in configuration file
            String resourcePath = "templates/" + TEMPLATE_MAPPING_CONFIG;
            String content = FileUtils.readResourceFile(resourcePath);
            parseMappingConfig(content);
            logger.info("Successfully loaded built-in default configuration");
        } catch (Exception e) {
            logger.error(
                    "Failed to load built-in default configuration, system cannot work properly: {}",
                    e.getMessage());
            throw new RuntimeException(
                    "Unable to load template mapping configuration file, please check if "
                            + TEMPLATE_MAPPING_CONFIG
                            + " file exists",
                    e);
        }
    }

    /** Get corresponding source template path based on reader type */
    public String getSourceTemplate(String readerType) {
        if (sourceMappings == null) {
            logger.warn("Source mappings not initialized, using default template");
            return "datax/sources/jdbc-source.conf";
        }

        String template = sourceMappings.get(readerType.toLowerCase());
        if (template == null) {
            logger.warn(
                    "Template mapping not found for reader type {}, using default template",
                    readerType);
            return "datax/sources/jdbc-source.conf";
        }

        logger.debug("Selected template for reader type {}: {}", readerType, template);
        return template;
    }

    /** Get corresponding sink template path based on writer type */
    public String getSinkTemplate(String writerType) {
        if (sinkMappings == null) {
            logger.warn("Sink mappings not initialized, using default template");
            return "datax/sinks/hdfs-sink.conf";
        }

        String template = sinkMappings.get(writerType.toLowerCase());
        if (template == null) {
            logger.warn(
                    "Template mapping not found for writer type {}, using default template",
                    writerType);
            return "datax/sinks/hdfs-sink.conf";
        }

        logger.debug("Selected template for writer type {}: {}", writerType, template);
        return template;
    }

    /** Get corresponding environment template path based on job type */
    public String getEnvTemplate(String jobType) {
        if (envMappings == null) {
            logger.warn("Environment mappings not initialized, using default template");
            return "datax/env/batch-env.conf";
        }

        String template = envMappings.get(jobType.toLowerCase());
        if (template == null) {
            logger.warn(
                    "Environment template mapping not found for job type {}, using default template",
                    jobType);
            return "datax/env/batch-env.conf";
        }

        logger.debug("Selected environment template for job type {}: {}", jobType, template);
        return template;
    }

    /** Get transformer configuration */
    @SuppressWarnings("unchecked")
    public Map<String, String> getTransformer(String transformerName) {
        if (transformers == null) {
            logger.warn("Transformer configuration not initialized");
            return new HashMap<>();
        }

        Object transformer = transformers.get(transformerName);
        if (transformer instanceof Map) {
            return (Map<String, String>) transformer;
        }

        logger.warn("Transformer not found: {}", transformerName);
        return new HashMap<>();
    }

    /** Check if specified reader type is supported */
    public boolean isReaderSupported(String readerType) {
        return sourceMappings != null && sourceMappings.containsKey(readerType.toLowerCase());
    }

    /** Check if specified writer type is supported */
    public boolean isWriterSupported(String writerType) {
        return sinkMappings != null && sinkMappings.containsKey(writerType.toLowerCase());
    }

    /** Get all supported reader types */
    public String[] getSupportedReaders() {
        if (sourceMappings == null) {
            return new String[0];
        }
        return sourceMappings.keySet().toArray(new String[0]);
    }

    /** Get all supported writer types */
    public String[] getSupportedWriters() {
        if (sinkMappings == null) {
            return new String[0];
        }
        return sinkMappings.keySet().toArray(new String[0]);
    }

    /** Reload configuration (for dynamic updates) */
    public void reload() {
        logger.info("Reloading template mapping configuration...");
        loadMappingConfig();
    }
}
