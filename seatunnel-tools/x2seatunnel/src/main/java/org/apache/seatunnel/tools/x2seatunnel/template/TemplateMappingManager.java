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

/** 模板映射配置管理器 负责加载和管理template-mapping.yaml配置文件 */
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

    /** 加载模板映射配置 */
    @SuppressWarnings("unchecked")
    private void loadMappingConfig() {
        logger.info("正在加载模板映射配置...");

        try {
            // 1. 尝试从文件系统加载
            String configPath = PathResolver.resolveTemplatePath(TEMPLATE_MAPPING_CONFIG);
            if (configPath != null && PathResolver.exists(configPath)) {
                logger.info("从文件系统加载模板映射配置: {}", configPath);
                String content = FileUtils.readFile(configPath);
                parseMappingConfig(content);
                return;
            }

            // 2. 从classpath加载（内置配置）
            String resourcePath = "templates/" + TEMPLATE_MAPPING_CONFIG;
            logger.info("从classpath加载模板映射配置: {}", resourcePath);
            String content = FileUtils.readResourceFile(resourcePath);
            parseMappingConfig(content);

        } catch (Exception e) {
            logger.error("加载模板映射配置失败: {}", e.getMessage(), e);
            // 使用默认配置
            initDefaultMappings();
        }
    }

    /** 解析映射配置内容 */
    @SuppressWarnings("unchecked")
    private void parseMappingConfig(String content) {
        Yaml yaml = new Yaml();
        mappingConfig = yaml.load(content);

        if (mappingConfig != null && mappingConfig.containsKey("datax")) {
            Map<String, Object> dataxConfig = (Map<String, Object>) mappingConfig.get("datax");

            // 加载source映射
            if (dataxConfig.containsKey("source_mappings")) {
                sourceMappings = (Map<String, String>) dataxConfig.get("source_mappings");
                logger.info("加载了 {} 个source映射", sourceMappings.size());
            }

            // 加载sink映射
            if (dataxConfig.containsKey("sink_mappings")) {
                sinkMappings = (Map<String, String>) dataxConfig.get("sink_mappings");
                logger.info("加载了 {} 个sink映射", sinkMappings.size());
            }

            // 加载环境映射
            if (dataxConfig.containsKey("env_mappings")) {
                envMappings = (Map<String, String>) dataxConfig.get("env_mappings");
                logger.info("加载了 {} 个环境映射", envMappings.size());
            }
        }

        // 加载转换器配置
        if (mappingConfig != null && mappingConfig.containsKey("transformers")) {
            transformers = (Map<String, Object>) mappingConfig.get("transformers");
            logger.info("加载了 {} 个转换器", transformers.size());
        }

        logger.info("模板映射配置加载完成");
    }

    /** 初始化默认映射（fallback） - 使用内置配置文件 */
    private void initDefaultMappings() {
        logger.warn("使用内置默认模板映射配置");

        try {
            // 尝试从内置配置文件加载默认配置
            String resourcePath = "templates/" + TEMPLATE_MAPPING_CONFIG;
            String content = FileUtils.readResourceFile(resourcePath);
            parseMappingConfig(content);
            logger.info("成功加载内置默认配置");
        } catch (Exception e) {
            logger.error("加载内置默认配置失败，系统无法正常工作: {}", e.getMessage());
            throw new RuntimeException(
                    "无法加载模板映射配置文件，请检查 " + TEMPLATE_MAPPING_CONFIG + " 文件是否存在", e);
        }
    }

    /** 根据reader类型获取对应的source模板路径 */
    public String getSourceTemplate(String readerType) {
        if (sourceMappings == null) {
            logger.warn("source映射未初始化，使用默认模板");
            return "datax/sources/jdbc-source.conf";
        }

        String template = sourceMappings.get(readerType.toLowerCase());
        if (template == null) {
            logger.warn("未找到reader类型 {} 的模板映射，使用默认模板", readerType);
            return "datax/sources/jdbc-source.conf";
        }

        logger.debug("为reader类型 {} 选择模板: {}", readerType, template);
        return template;
    }

    /** 根据writer类型获取对应的sink模板路径 */
    public String getSinkTemplate(String writerType) {
        if (sinkMappings == null) {
            logger.warn("sink映射未初始化，使用默认模板");
            return "datax/sinks/hdfs-sink.conf";
        }

        String template = sinkMappings.get(writerType.toLowerCase());
        if (template == null) {
            logger.warn("未找到writer类型 {} 的模板映射，使用默认模板", writerType);
            return "datax/sinks/hdfs-sink.conf";
        }

        logger.debug("为writer类型 {} 选择模板: {}", writerType, template);
        return template;
    }

    /** 根据任务类型获取对应的环境模板路径 */
    public String getEnvTemplate(String jobType) {
        if (envMappings == null) {
            logger.warn("环境映射未初始化，使用默认模板");
            return "datax/env/batch-env.conf";
        }

        String template = envMappings.get(jobType.toLowerCase());
        if (template == null) {
            logger.warn("未找到任务类型 {} 的环境模板映射，使用默认模板", jobType);
            return "datax/env/batch-env.conf";
        }

        logger.debug("为任务类型 {} 选择环境模板: {}", jobType, template);
        return template;
    }

    /** 获取转换器配置 */
    @SuppressWarnings("unchecked")
    public Map<String, String> getTransformer(String transformerName) {
        if (transformers == null) {
            logger.warn("转换器配置未初始化");
            return new HashMap<>();
        }

        Object transformer = transformers.get(transformerName);
        if (transformer instanceof Map) {
            return (Map<String, String>) transformer;
        }

        logger.warn("未找到转换器: {}", transformerName);
        return new HashMap<>();
    }

    /** 检查是否支持指定的reader类型 */
    public boolean isReaderSupported(String readerType) {
        return sourceMappings != null && sourceMappings.containsKey(readerType.toLowerCase());
    }

    /** 检查是否支持指定的writer类型 */
    public boolean isWriterSupported(String writerType) {
        return sinkMappings != null && sinkMappings.containsKey(writerType.toLowerCase());
    }

    /** 获取所有支持的reader类型 */
    public String[] getSupportedReaders() {
        if (sourceMappings == null) {
            return new String[0];
        }
        return sourceMappings.keySet().toArray(new String[0]);
    }

    /** 获取所有支持的writer类型 */
    public String[] getSupportedWriters() {
        if (sinkMappings == null) {
            return new String[0];
        }
        return sinkMappings.keySet().toArray(new String[0]);
    }

    /** 重新加载配置（用于动态更新） */
    public void reload() {
        logger.info("重新加载模板映射配置...");
        loadMappingConfig();
    }
}
