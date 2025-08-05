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

import org.apache.seatunnel.tools.x2seatunnel.model.MappingTracker;
import org.apache.seatunnel.tools.x2seatunnel.util.DataXFieldExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Template variable resolver - supports basic variables, default values, conditional mapping and
 * transformer calls
 */
public class TemplateVariableResolver {

    private static final Logger logger = LoggerFactory.getLogger(TemplateVariableResolver.class);

    // Constant definitions
    private static final String DATAX_PREFIX = "datax.";
    private static final String DATAX_JOB_PREFIX = "datax.job.";
    private static final int DATAX_PREFIX_LENGTH = 6;
    private static final String JOB_PREFIX = "job.";
    private static final int INDENT_SIZE = 2;
    private static final int TAB_SIZE = 4;
    private static final String DEFAULT_JOIN_SEPARATOR = ",";
    private static final String DEFAULT_SPLIT_DELIMITER = "/";

    // Common string constants
    private static final String EMPTY_STRING = "";
    private static final String EQUALS_SIGN = "=";
    private static final String PIPE_SYMBOL = "|";
    private static final String OPEN_BRACE = "{";
    private static final String CLOSE_BRACE = "}";
    private static final String COMMENT_PREFIX = "#";
    private static final String NEWLINE = "\n";
    private static final String QUOTE_DOUBLE = "\"";
    private static final String QUOTE_SINGLE = "'";
    private static final String TEMPLATE_VAR_START = "{{";
    private static final String TEMPLATE_VAR_END = "}}";

    // Log message constants
    private static final String LOG_MSG_TEMPLATE_RESOLUTION_START =
            "Starting template variable resolution";
    private static final String LOG_MSG_TEMPLATE_RESOLUTION_COMPLETE =
            "Template variable resolution completed";
    private static final String LOG_MSG_JINJA2_RESOLUTION_COMPLETE =
            "Jinja2 variable resolution completed";
    private static final String LOG_MSG_TEMPLATE_ANALYSIS_COMPLETE =
            "Template analysis resolution completed, total fields: {}";

    // Error message constants
    private static final String ERROR_MSG_TEMPLATE_RESOLUTION_FAILED =
            "Template variable resolution failed";
    private static final String ERROR_MSG_TEMPLATE_ANALYSIS_FAILED =
            "Template analysis resolution failed";

    // Jinja2 variable pattern: {{ datax.path.to.value }}
    private static final Pattern JINJA2_VARIABLE_PATTERN =
            Pattern.compile("\\{\\{\\s*([^}|]+)\\s*\\}\\}");

    // Jinja2 filter pattern: {{ datax.path.to.value | filter }}
    private static final Pattern JINJA2_FILTER_PATTERN =
            Pattern.compile("\\{\\{\\s*([^}|]+)\\s*\\|\\s*([^}]+)\\s*\\}\\}");

    // Other patterns
    private static final Pattern SET_PATTERN =
            Pattern.compile("\\{%\\s*set\\s+(\\w+)\\s*=\\s*(.*?)\\s*%\\}");
    private static final Pattern FILTER_PATTERN =
            Pattern.compile("\\|\\s*([a-zA-Z_][a-zA-Z0-9_]*)");

    private final ObjectMapper objectMapper;
    private final TemplateMappingManager templateMappingManager;
    private final MappingTracker mappingTracker;

    // Current parsing context: records the target field path being parsed
    private String currentTargetContext = null;

    // Flag: whether currently processing complex transformation (compound expressions containing
    // filters)
    private boolean processingComplexTransform = false;

    // Flag: suppress missing field recording when encountering default filter
    private boolean suppressMissing = false;

    // Field reference tracker
    private DataXFieldExtractor.FieldReferenceTracker fieldReferenceTracker;

    /**
     * Constructor - supports full functionality
     *
     * @param templateMappingManager template mapping manager, can be null
     * @param mappingTracker mapping tracker, can be null
     */
    public TemplateVariableResolver(
            TemplateMappingManager templateMappingManager, MappingTracker mappingTracker) {
        this.objectMapper = createObjectMapper();
        this.templateMappingManager = templateMappingManager;
        this.mappingTracker = mappingTracker;
    }

    /**
     * Constructor - supports template mapping manager only
     *
     * @param templateMappingManager template mapping manager, can be null
     */
    public TemplateVariableResolver(TemplateMappingManager templateMappingManager) {
        this(templateMappingManager, null);
    }

    /** Default constructor - basic functionality */
    public TemplateVariableResolver() {
        this(null, null);
    }

    /**
     * Create and configure ObjectMapper instance
     *
     * @return configured ObjectMapper instance
     */
    private static ObjectMapper createObjectMapper() {
        return new ObjectMapper();
    }

    /**
     * Check if template content is empty
     *
     * @param templateContent template content
     * @return true if empty
     */
    private boolean isEmptyTemplate(String templateContent) {
        return templateContent == null || templateContent.trim().isEmpty();
    }

    /**
     * Core method for template resolution
     *
     * @param templateContent template content
     * @param rootNode JSON root node
     * @return resolved content
     */
    private String resolveTemplate(String templateContent, JsonNode rootNode) {
        String result = templateContent;

        // 1. Process {% set var = expr %} syntax (supports simple expressions only)
        Map<String, String> localVars = processSetStatements(result, rootNode);
        result = SET_PATTERN.matcher(result).replaceAll("");

        // 2. Simple string replacement for local variables
        result = replaceLocalVariables(result, localVars);

        // 3. Use smart context resolution to handle all variables
        result = resolveWithSmartContext(result, rootNode);

        logger.debug(LOG_MSG_TEMPLATE_RESOLUTION_COMPLETE);
        return result;
    }

    /**
     * Process {% set var = expr %} statements
     *
     * @param content template content
     * @param rootNode JSON root node
     * @return local variable mapping
     */
    private Map<String, String> processSetStatements(String content, JsonNode rootNode) {
        Map<String, String> localVars = new HashMap<>();
        Matcher setMatcher = SET_PATTERN.matcher(content);

        while (setMatcher.find()) {
            String varName = setMatcher.group(1);
            String expr = setMatcher.group(2);
            String exprTemplate = "{{ " + expr + " }}";
            String value =
                    resolveJinja2FilterVariables(
                            resolveJinja2Variables(exprTemplate, rootNode), rootNode);
            localVars.put(varName, value);
            logger.debug("Setting local variable: {} = {}", varName, value);
        }

        return localVars;
    }

    /**
     * Replace local variables
     *
     * @param content template content
     * @param localVars local variable mapping
     * @return content after replacement
     */
    private String replaceLocalVariables(String content, Map<String, String> localVars) {
        String result = content;
        for (Map.Entry<String, String> entry : localVars.entrySet()) {
            result = result.replace("{{ " + entry.getKey() + " }}", entry.getValue());
        }
        return result;
    }

    /**
     * Normalize DataX path, remove datax prefix and convert to job prefix
     *
     * @param path original path
     * @return normalized path
     */
    private String normalizeDataXPath(String path) {
        if (path.startsWith(DATAX_JOB_PREFIX)) {
            return path.substring(DATAX_PREFIX_LENGTH);
        } else if (path.startsWith(DATAX_PREFIX)) {
            return path.replace(DATAX_PREFIX, JOB_PREFIX);
        }
        return path;
    }

    /**
     * Unified method for handling template resolution exceptions
     *
     * @param operation operation description
     * @param e original exception
     * @throws TemplateResolutionException wrapped exception
     */
    private void handleTemplateException(String operation, Exception e) {
        String errorMsg = operation + ": " + e.getMessage();
        logger.error(errorMsg, e);
        throw new TemplateResolutionException(errorMsg, e);
    }

    /** Template resolution exception */
    public static class TemplateResolutionException extends RuntimeException {
        public TemplateResolutionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Parse template variables (using raw JSON string)
     *
     * @param templateContent template content
     * @param dataXJsonContent DataX JSON configuration content
     * @return parsed content
     */
    public String resolve(String templateContent, String dataXJsonContent) {
        if (isEmptyTemplate(templateContent)) {
            return templateContent;
        }

        logger.debug(LOG_MSG_TEMPLATE_RESOLUTION_START);

        try {
            // Parse JSON string directly to JsonNode
            JsonNode rootNode = objectMapper.readTree(dataXJsonContent);
            return resolveWithSmartContext(templateContent, rootNode);

        } catch (Exception e) {
            handleTemplateException(ERROR_MSG_TEMPLATE_RESOLUTION_FAILED, e);
            return null; // This line won't execute, but compiler needs it
        }
    }

    /** Parse Jinja2 style basic variables: {{ datax.path.to.value }} */
    private String resolveJinja2Variables(String content, JsonNode rootNode) {
        logger.debug(
                "Starting to parse Jinja2 variables, content length: {}, fieldReferenceTracker: {}",
                content.length(),
                fieldReferenceTracker != null ? "set" : "not set");

        Matcher matcher = JINJA2_VARIABLE_PATTERN.matcher(content);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            String path = matcher.group(1).trim();
            String value = extractValueFromJinja2Path(rootNode, path);
            String resolvedValue = (value != null) ? value : EMPTY_STRING;

            logger.debug("Found variable: {}, resolved value: {}", path, resolvedValue);

            // Increment field reference count
            if (fieldReferenceTracker != null && path.startsWith(DATAX_PREFIX)) {
                String normalizedPath = normalizeDataXPath(path);
                logger.debug(
                        "Incrementing reference count when resolving variable: {} -> {}",
                        path,
                        normalizedPath);
                incrementFieldReference(normalizedPath);
            } else {
                logger.debug(
                        "Skipping reference count: fieldReferenceTracker={}, path={}",
                        fieldReferenceTracker != null ? "set" : "not set",
                        path);
            }

            matcher.appendReplacement(sb, Matcher.quoteReplacement(resolvedValue));
        }
        matcher.appendTail(sb);

        logger.debug(LOG_MSG_JINJA2_RESOLUTION_COMPLETE);
        return sb.toString();
    }

    /** Parse Jinja2 style filter variables: {{ datax.path.to.value | filter }} */
    private String resolveJinja2FilterVariables(String content, JsonNode rootNode) {
        logger.debug("Starting to resolve filter variables, content: {}", content.trim());
        Matcher matcher = JINJA2_FILTER_PATTERN.matcher(content);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            String path = matcher.group(1).trim();
            String filterExpression = matcher.group(2).trim();

            logger.debug("Found filter variable: {}, filter: {}", path, filterExpression);

            // Increment field reference count
            if (fieldReferenceTracker != null && path.startsWith(DATAX_PREFIX)) {
                String normalizedPath = normalizeDataXPath(path);
                logger.debug(
                        "Incrementing reference count for filter variable: {} -> {}",
                        path,
                        normalizedPath);
                incrementFieldReference(normalizedPath);
            }

            // Parse filter chain: filter1 | filter2 | filter3
            String[] filters = parseFilterChain(filterExpression);
            // If the first filter is default, suppress missing field recording
            boolean needSuppress = filters.length > 0 && filters[0].startsWith("default");
            if (needSuppress) {
                this.suppressMissing = true;
            }
            // Extract original value
            String value = extractValueFromJinja2Path(rootNode, path);
            if (needSuppress) {
                this.suppressMissing = false;
            }

            Object resolvedValue = value;

            for (String filter : filters) {
                // Add null check to prevent null pointer exception
                if (resolvedValue == null) {
                    resolvedValue = EMPTY_STRING;
                }

                // Apply filter uniformly
                resolvedValue = applyFilter(resolvedValue, filter.trim());
            }

            String finalValue =
                    resolvedValue instanceof String
                            ? (String) resolvedValue
                            : (resolvedValue != null ? resolvedValue.toString() : EMPTY_STRING);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(finalValue));
        }
        matcher.appendTail(sb);

        return sb.toString();
    }

    /** Intelligently parse filter chain, correctly handle pipe symbols within parentheses */
    private String[] parseFilterChain(String filterExpression) {
        List<String> filters = new ArrayList<>();
        StringBuilder currentFilter = new StringBuilder();
        int depth = 0;
        boolean inQuotes = false;
        char quoteChar = '\0';

        for (int i = 0; i < filterExpression.length(); i++) {
            char c = filterExpression.charAt(i);

            if (!inQuotes && (c == '\'' || c == '"')) {
                inQuotes = true;
                quoteChar = c;
                currentFilter.append(c);
            } else if (inQuotes && c == quoteChar) {
                inQuotes = false;
                quoteChar = '\0';
                currentFilter.append(c);
            } else if (!inQuotes && c == '(') {
                depth++;
                currentFilter.append(c);
            } else if (!inQuotes && c == ')') {
                depth--;
                currentFilter.append(c);
            } else if (!inQuotes && c == '|' && depth == 0) {
                filters.add(currentFilter.toString().trim());
                currentFilter.setLength(0);
            } else {
                currentFilter.append(c);
            }
        }

        if (currentFilter.length() > 0) {
            filters.add(currentFilter.toString().trim());
        }

        return filters.toArray(new String[0]);
    }

    /** Extract value from Jinja2 style path: datax.job.content[0].reader.parameter.column */
    private String extractValueFromJinja2Path(JsonNode rootNode, String path) {
        try {
            JsonNode currentNode = rootNode;

            // Convert datax.job.content[0] to job.content[0] (remove datax prefix)
            if (path.startsWith(DATAX_PREFIX)) {
                path = path.substring(DATAX_PREFIX_LENGTH);
            }

            String[] pathParts = path.split("\\.");

            for (String part : pathParts) {
                if (currentNode == null) {
                    // Record missing field
                    if (mappingTracker != null && !suppressMissing) {
                        mappingTracker.recordMissingField(
                                path, "Field not found in DataX configuration");
                    }
                    return null;
                }

                // Handle array index, such as content[0]
                if (part.contains("[") && part.contains("]")) {
                    String arrayName = part.substring(0, part.indexOf("["));
                    String indexStr = part.substring(part.indexOf("[") + 1, part.indexOf("]"));

                    currentNode = currentNode.get(arrayName);
                    if (currentNode != null && currentNode.isArray()) {
                        try {
                            int index = Integer.parseInt(indexStr);
                            currentNode = currentNode.get(index);
                        } catch (NumberFormatException e) {
                            logger.warn("Invalid array index: {}", indexStr);
                            if (mappingTracker != null && !suppressMissing) {
                                mappingTracker.recordMissingField(
                                        path, "Invalid array index: " + indexStr);
                            }
                            return null;
                        }
                    }
                } else {
                    currentNode = currentNode.get(part);
                }
            }

            if (currentNode != null && !currentNode.isNull()) {
                String value;
                if (currentNode.isArray()) {
                    // If it's an array, return all elements of the array
                    StringBuilder result = new StringBuilder();
                    for (int i = 0; i < currentNode.size(); i++) {
                        if (i > 0) result.append(",");
                        result.append(currentNode.get(i).asText());
                    }
                    value = result.toString();
                } else {
                    value = currentNode.asText();
                }

                // Record successful field extraction, unless suppressed or part of complex
                // transformation
                if (mappingTracker != null
                        && !suppressMissing
                        && value != null
                        && !value.isEmpty()
                        && !isPartOfComplexTransform()) {
                    mappingTracker.recordDirectMapping(
                            path, currentTargetContext, value, "Directly extracted from DataX");
                }

                return value;
            } else {
                // Record missing field
                if (mappingTracker != null && !suppressMissing) {
                    mappingTracker.recordMissingField(
                            path, "Field value is empty in DataX configuration");
                }
            }

        } catch (Exception e) {
            logger.warn("Failed to extract Jinja2 path value: {}", path, e);
            if (mappingTracker != null && !suppressMissing) {
                mappingTracker.recordMissingField(path, "Extraction failed: " + e.getMessage());
            }
        }

        return null;
    }

    /** Find matching right parenthesis position, handle nested parentheses */
    private int findMatchingCloseParen(String text, int openParenPos) {
        int depth = 1;
        for (int i = openParenPos + 1; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1; // No matching right parenthesis found
    }

    /** Unified filter application method - supports strings and arrays */
    private Object applyFilter(Object value, String filterExpression) {
        if (value == null) {
            value = EMPTY_STRING;
        }

        // Parse filter: join(',') or join(', ') or default('SELECT * FROM table')
        String filterName;
        String filterArgs = EMPTY_STRING;

        if (filterExpression.contains("(") && filterExpression.contains(")")) {
            filterName = filterExpression.substring(0, filterExpression.indexOf("(")).trim();

            // Find correct right parenthesis position (handle nested parentheses)
            int openParenPos = filterExpression.indexOf("(");
            int closeParenPos = findMatchingCloseParen(filterExpression, openParenPos);

            if (closeParenPos != -1) {
                filterArgs = filterExpression.substring(openParenPos + 1, closeParenPos).trim();
                // Remove quotes
                if (filterArgs.startsWith(QUOTE_SINGLE) && filterArgs.endsWith(QUOTE_SINGLE)) {
                    filterArgs = filterArgs.substring(1, filterArgs.length() - 1);
                } else if (filterArgs.startsWith(QUOTE_DOUBLE)
                        && filterArgs.endsWith(QUOTE_DOUBLE)) {
                    filterArgs = filterArgs.substring(1, filterArgs.length() - 1);
                }
            } else {
                logger.warn("Unable to find matching closing parenthesis: {}", filterExpression);
            }
        } else {
            filterName = filterExpression.trim();
        }

        // Record original value for comparison to see if transformation occurred
        Object originalValue = value;

        // Apply filter
        Object result;
        switch (filterName) {
            case "join":
                if (value instanceof String[]) {
                    result =
                            applyJoinFilterOnArray(
                                    (String[]) value,
                                    filterArgs.isEmpty() ? DEFAULT_JOIN_SEPARATOR : filterArgs);
                } else {
                    result =
                            applyJoinFilter(
                                    value.toString(),
                                    filterArgs.isEmpty() ? DEFAULT_JOIN_SEPARATOR : filterArgs);
                }
                break;
            case "default":
                String stringValue = value.toString();
                boolean usedDefaultValue = stringValue.isEmpty();
                result = usedDefaultValue ? filterArgs : stringValue;

                // Record whether default value was used for subsequent mapping recording
                if (mappingTracker != null && !isPartOfComplexTransform()) {
                    if (usedDefaultValue) {
                        // Used default value
                        mappingTracker.recordDefaultValue(
                                currentTargetContext,
                                result.toString(),
                                "Applied default value: " + filterArgs);
                    } else {
                        // Used original value, belongs to direct mapping
                        mappingTracker.recordDirectMapping(
                                null,
                                currentTargetContext,
                                result.toString(),
                                "Used original value, default value not applied");
                    }
                }
                break;
            case "upper":
                result = value.toString().toUpperCase();
                break;
            case "lower":
                result = value.toString().toLowerCase();
                break;
            case "regex_extract":
                {
                    // Use original filterExpression to extract parameters, ensuring quotes and
                    // commas are included
                    int lpos = filterExpression.indexOf('(');
                    int rpos = findMatchingCloseParen(filterExpression, lpos);
                    String rawArgs = filterExpression.substring(lpos + 1, rpos);
                    String extractedVal = applyRegexExtract(value.toString(), rawArgs);
                    result = extractedVal;
                    // Record regex extraction transformation, only once
                    if (mappingTracker != null
                            && !equals(originalValue, result)
                            && !isPartOfComplexTransform()) {
                        mappingTracker.recordTransformMapping(
                                null, currentTargetContext, result.toString(), filterName);
                    }
                }
                break;
            case "jdbc_driver_mapper":
                result = applyTransformer(value.toString(), "jdbc_driver_mapper");
                break;
            case "split":
                result = applySplit(value.toString(), filterArgs);
                break;
            case "get":
                result = applyGet(value, filterArgs);
                break;
            case "replace":
                result = applyReplace(value.toString(), filterArgs);
                break;
            default:
                // Check if it's a transformer call
                if (templateMappingManager != null
                        && templateMappingManager.getTransformer(filterName) != null) {
                    result = applyTransformer(value.toString(), filterName);
                } else {
                    logger.warn("Unsupported filter: {}", filterName);
                    result = value;
                }
        }

        // Record field transformation (if transformation occurred)
        if (mappingTracker != null && !equals(originalValue, result)) {
            if ("regex_extract".equals(filterName)) {
                // Already recorded in regex_extract case, skip duplicate recording
            } else if ("default".equals(filterName)) {
                // Default filter mapping record already handled in case, skip duplicate recording
            } else if (!isPartOfComplexTransform()) {
                // Other filter transformations
                mappingTracker.recordTransformMapping(
                        null, currentTargetContext, result.toString(), filterName);
            }
        }

        return result;
    }

    /** Determine if two objects are equal */
    private boolean equals(Object obj1, Object obj2) {
        if (obj1 == null && obj2 == null) return true;
        if (obj1 == null || obj2 == null) return false;
        return obj1.toString().equals(obj2.toString());
    }

    /** Apply transformer */
    private String applyTransformer(String value, String transformerName) {
        if (templateMappingManager == null) {
            logger.warn(
                    "TemplateMappingManager not initialized, cannot use transformer: {}",
                    transformerName);
            return value;
        }

        try {
            Map<String, String> transformer =
                    templateMappingManager.getTransformer(transformerName);
            if (transformer == null) {
                logger.warn("Transformer does not exist: {}", transformerName);
                return value;
            }

            logger.info("Applying transformer {} to process value: {}", transformerName, value);
            logger.info("Transformer mapping table: {}", transformer);

            // Find matching transformer rules
            for (Map.Entry<String, String> entry : transformer.entrySet()) {
                String pattern = entry.getKey();
                String mappedValue = entry.getValue();

                // Support contains matching
                if (value.toLowerCase().contains(pattern.toLowerCase())) {
                    logger.info(
                            "Transformer {} matched successfully: {} -> {}",
                            transformerName,
                            value,
                            mappedValue);
                    return mappedValue;
                }
            }

            logger.warn(
                    "Transformer {} found no match, returning original value: {}",
                    transformerName,
                    value);
            return value;

        } catch (Exception e) {
            logger.error("Failed to apply transformer: {}", transformerName, e);
            return value;
        }
    }

    /** Apply join filter */
    private String applyJoinFilter(String value, String separator) {
        if (value == null || value.trim().isEmpty()) {
            return "";
        }

        // If the value itself is a comma-separated string, directly join with specified separator
        if (value.contains(",")) {
            String[] parts = value.split(",");
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) result.append(separator);
                result.append(parts[i].trim());
            }
            return result.toString();
        }

        return value;
    }

    /** Apply regular expression extraction filter */
    private String applyRegexExtract(String value, String regexPattern) {
        if (value == null
                || value.trim().isEmpty()
                || regexPattern == null
                || regexPattern.trim().isEmpty()) {
            return value;
        }

        try {
            logger.info(
                    "Regular expression extraction: input value='{}', parameters='{}'",
                    value,
                    regexPattern);

            // Support two formats:
            // 1. Simple mode: regex_extract('pattern') - extract first matching group
            // 2. Replacement mode: regex_extract('pattern', 'replacement') - use replacement
            // pattern

            // Parse parameters, considering commas within quotes should not be split
            String[] parts = parseRegexArgs(regexPattern);
            String pattern = parts[0].trim();
            String replacement = parts.length > 1 ? parts[1].trim() : "$1";

            logger.info(
                    "Regular expression extraction: pattern='{}', replacement='{}', input value='{}'",
                    pattern,
                    replacement,
                    value);

            java.util.regex.Pattern compiledPattern = java.util.regex.Pattern.compile(pattern);
            java.util.regex.Matcher matcher = compiledPattern.matcher(value);

            if (matcher.find()) {
                // If replacement only contains group references, concatenate and return
                // corresponding groups
                if (replacement.matches("(\\$\\d+)(\\.\\$\\d+)*")) {
                    String extracted = replacement;
                    // Replace group references
                    for (int i = 1; i <= matcher.groupCount(); i++) {
                        extracted = extracted.replace("$" + i, matcher.group(i));
                    }
                    logger.info("Regular expression extraction successful: result='{}'", extracted);
                    return extracted;
                } else {
                    String replaced = matcher.replaceFirst(replacement);
                    logger.info("Regular expression replacement successful: result='{}'", replaced);
                    return replaced;
                }
            } else {
                logger.warn(
                        "Regular expression extraction failed: pattern '{}' does not match input value '{}'",
                        pattern,
                        value);
                return value;
            }

        } catch (Exception e) {
            logger.error(
                    "Regular expression extraction error: pattern='{}', value='{}'",
                    regexPattern,
                    value,
                    e);
            return value;
        }
    }

    /** Parse regex_extract parameters, correctly handle commas within quotes */
    private String[] parseRegexArgs(String args) {
        if (args == null || args.trim().isEmpty()) {
            return new String[0];
        }

        List<String> result = new ArrayList<>();
        StringBuilder currentArg = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = '\0';

        for (int i = 0; i < args.length(); i++) {
            char c = args.charAt(i);

            if (!inQuotes && (c == '\'' || c == '"')) {
                inQuotes = true;
                quoteChar = c;
            } else if (inQuotes && c == quoteChar) {
                inQuotes = false;
                quoteChar = '\0';
            } else if (!inQuotes && c == ',') {
                result.add(currentArg.toString().trim());
                currentArg.setLength(0);
                continue;
            }

            currentArg.append(c);
        }

        if (currentArg.length() > 0) {
            result.add(currentArg.toString().trim());
        }

        // Remove quotes from each parameter
        for (int i = 0; i < result.size(); i++) {
            String arg = result.get(i);
            if ((arg.startsWith("'") && arg.endsWith("'"))
                    || (arg.startsWith("\"") && arg.endsWith("\""))) {
                result.set(i, arg.substring(1, arg.length() - 1));
            }
        }

        return result.toArray(new String[0]);
    }

    /**
     * Apply split filter - string splitting
     *
     * @param value input string
     * @param delimiter delimiter, default is "/"
     * @return split string array
     */
    private String[] applySplit(String value, String delimiter) {
        if (value == null || value.trim().isEmpty()) {
            return new String[0];
        }

        // If no delimiter is specified, use default delimiter
        String actualDelimiter =
                (delimiter != null && !delimiter.trim().isEmpty())
                        ? delimiter.trim()
                        : DEFAULT_SPLIT_DELIMITER;

        logger.info("String splitting: input value='{}', delimiter='{}'", value, actualDelimiter);

        String[] result = value.split(actualDelimiter);
        logger.info("Split result: {}", java.util.Arrays.toString(result));

        return result;
    }

    /**
     * Apply get filter - get element at specified position in array
     *
     * @param value input value (may be string array)
     * @param indexStr index string, supports negative index
     * @return element at specified position
     */
    private String applyGet(Object value, String indexStr) {
        if (value == null) {
            return "";
        }

        // If not a string array, return string form directly
        if (!(value instanceof String[])) {
            return value.toString();
        }

        String[] array = (String[]) value;
        if (array.length == 0) {
            return "";
        }

        try {
            int index = Integer.parseInt(indexStr.trim());

            // Support negative index
            if (index < 0) {
                index = array.length + index;
            }

            if (index >= 0 && index < array.length) {
                String result = array[index];
                logger.info("Array get: index={}, result='{}'", indexStr, result);
                return result;
            } else {
                logger.warn(
                        "Array index out of range: index={}, array length={}",
                        indexStr,
                        array.length);
                return "";
            }
        } catch (NumberFormatException e) {
            logger.error("Invalid array index: {}", indexStr, e);
            return "";
        }
    }

    /**
     * Apply replace filter - string replacement
     *
     * @param value input string
     * @param args replacement parameters, format is "old,new"
     * @return replaced string
     */
    private String applyReplace(String value, String args) {
        if (value == null) {
            return "";
        }

        if (args == null || args.trim().isEmpty()) {
            return value;
        }

        // Parse replacement parameters, format is "old,new"
        String[] parts = args.split(",", 2);
        if (parts.length == 2) {
            String oldStr = parts[0].trim();
            String newStr = parts[1].trim();

            logger.info(
                    "String replacement: input value='{}', replace '{}' -> '{}'",
                    value,
                    oldStr,
                    newStr);

            String result = value.replace(oldStr, newStr);
            logger.info("Replacement result: '{}'", result);
            return result;
        } else {
            logger.warn(
                    "replace filter parameter format error, should be 'old,new', actual: {}", args);
            return value;
        }
    }

    /** Apply join filter to array */
    private String applyJoinFilterOnArray(String[] value, String separator) {
        if (value == null || value.length == 0) {
            return "";
        }

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < value.length; i++) {
            if (i > 0) {
                result.append(separator);
            }
            result.append(value[i] != null ? value[i].trim() : "");
        }
        return result.toString();
    }

    /**
     * Set current target context (for mapping tracking). This method can be called externally to
     * set context when parsing specific configuration sections
     */
    public void setCurrentTargetContext(String targetContext) {
        this.currentTargetContext = targetContext;
    }

    /** Clear current target context */
    public void clearCurrentTargetContext() {
        this.currentTargetContext = null;
    }

    /** Set field reference tracker */
    public void setFieldReferenceTracker(DataXFieldExtractor.FieldReferenceTracker tracker) {
        this.fieldReferenceTracker = tracker;
    }

    /** Get field reference tracker */
    public DataXFieldExtractor.FieldReferenceTracker getFieldReferenceTracker() {
        return this.fieldReferenceTracker;
    }

    /** Increment field reference count, supports intelligent matching of array fields */
    private void incrementFieldReference(String normalizedPath) {
        if (fieldReferenceTracker == null) {
            return;
        }

        // Directly referenced field
        fieldReferenceTracker.incrementReference(normalizedPath);
        logger.debug("Field reference count: {}", normalizedPath);

        // Handle bidirectional matching of array fields
        Map<String, String> allFields = fieldReferenceTracker.getAllFields();

        // Case 1: If referencing an array field, all elements of the array should also be marked as
        // referenced
        // For example: when referencing job.content[0].reader.parameter.connection[0].jdbcUrl,
        // also mark job.content[0].reader.parameter.connection[0].jdbcUrl[0], jdbcUrl[1] etc. as
        // referenced
        for (String fieldPath : allFields.keySet()) {
            if (isArrayElementOf(fieldPath, normalizedPath)) {
                fieldReferenceTracker.incrementReference(fieldPath);
                logger.debug(
                        "Array element reference count: {} (from array reference: {})",
                        fieldPath,
                        normalizedPath);
            }
        }

        // Case 2: If referencing an array element, the corresponding array itself should also be
        // marked as referenced
        // For example: when referencing job.content[0].reader.parameter.connection[0].jdbcUrl[0],
        // also mark job.content[0].reader.parameter.connection[0].jdbcUrl as referenced
        String arrayFieldName = getArrayFieldNameFromElement(normalizedPath);
        if (arrayFieldName != null && allFields.containsKey(arrayFieldName)) {
            fieldReferenceTracker.incrementReference(arrayFieldName);
            logger.debug(
                    "Array field reference count: {} (from array element reference: {})",
                    arrayFieldName,
                    normalizedPath);
        }
    }

    /**
     * Determine if fieldPath is an array element of arrayPath. For example:
     * job.content[0].reader.parameter.connection[0].jdbcUrl[0] is an element of
     * job.content[0].reader.parameter.connection[0].jdbcUrl
     */
    private boolean isArrayElementOf(String fieldPath, String arrayPath) {
        // Check if it's an array element pattern: arrayPath[index]
        if (fieldPath.startsWith(arrayPath + "[") && fieldPath.endsWith("]")) {
            // Extract index part, ensure it's a number
            String indexPart = fieldPath.substring(arrayPath.length() + 1, fieldPath.length() - 1);
            try {
                Integer.parseInt(indexPart);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * Extract array field name from array element path. For example:
     * job.content[0].reader.parameter.connection[0].jdbcUrl[0] ->
     * job.content[0].reader.parameter.connection[0].jdbcUrl
     */
    private String getArrayFieldNameFromElement(String elementPath) {
        // Check if it's an array element pattern: xxx[number]
        if (elementPath.matches(".*\\[\\d+\\]$")) {
            int lastBracket = elementPath.lastIndexOf('[');
            return elementPath.substring(0, lastBracket);
        }
        return null;
    }

    /** Check if line contains filters */
    private boolean containsFilters(String line) {
        return line.contains(PIPE_SYMBOL) && containsVariable(line);
    }

    /** Check if currently processing complex transformation */
    private boolean isPartOfComplexTransform() {
        return processingComplexTransform;
    }

    /** Check if it's a real complex transformation (multiple variables or complex expressions) */
    private boolean isReallyComplexTransform(String line) {
        // Count number of variables
        Pattern variablePattern = Pattern.compile("\\{\\{[^}]+\\}\\}");
        Matcher matcher = variablePattern.matcher(line);
        int variableCount = 0;
        while (matcher.find()) {
            variableCount++;
        }

        // If there are multiple variables, consider it a complex transformation
        if (variableCount > 1) {
            return true;
        }

        // If there's only one variable, check if there's a complex filter chain (more than 2
        // filters)
        if (variableCount == 1) {
            matcher.reset();
            if (matcher.find()) {
                String variable = matcher.group();
                // Count pipe symbols
                long pipeCount = variable.chars().filter(ch -> ch == '|').count();
                // If there are more than 2 filters, consider it a complex transformation
                return pipeCount > 2;
            }
        }

        return false;
    }

    /** Record complex transformation mapping (lines containing multiple variables and filters) */
    private void recordComplexTransformMapping(
            String originalLine, String resolvedLine, String targetContext) {
        if (mappingTracker == null) {
            return;
        }

        // Extract original template expression
        String templateExpression = extractTemplateExpression(originalLine);

        // Extract final value
        String finalValue = extractFinalValue(resolvedLine);

        // Extract list of filters used
        String filtersUsed = extractFiltersFromExpression(templateExpression);

        // Escape template expression for Markdown
        String escapedTemplateExpression = escapeMarkdownTableContent(templateExpression);

        // Record as transformation mapping, using escaped template expression as source
        mappingTracker.recordTransformMapping(
                escapedTemplateExpression, targetContext, finalValue, filtersUsed);

        logger.debug(
                "Record complex transformation mapping: {} -> {} = {}",
                escapedTemplateExpression,
                targetContext,
                finalValue);
    }

    /** Extract template expression */
    private String extractTemplateExpression(String line) {
        // Extract part after =, remove quotes
        if (line.contains("=")) {
            String value = line.substring(line.indexOf("=") + 1).trim();
            if (value.startsWith("\"") && value.endsWith("\"")) {
                value = value.substring(1, value.length() - 1);
            }
            return value;
        }
        return line.trim();
    }

    /** Extract final value */
    private String extractFinalValue(String resolvedLine) {
        if (resolvedLine.contains("=")) {
            String value = resolvedLine.substring(resolvedLine.indexOf("=") + 1).trim();
            if (value.startsWith("\"") && value.endsWith("\"")) {
                value = value.substring(1, value.length() - 1);
            }
            return value;
        }
        return resolvedLine.trim();
    }

    /** Extract filter list from template expression */
    private String extractFiltersFromExpression(String templateExpression) {
        if (templateExpression == null || !templateExpression.contains("|")) {
            return "";
        }

        Set<String> filters = new HashSet<>();
        Matcher matcher = FILTER_PATTERN.matcher(templateExpression);

        while (matcher.find()) {
            String filter = matcher.group(1);
            filters.add(filter);
        }

        // Convert filter list to string, separated by commas
        return String.join(", ", filters);
    }

    /** Escape Markdown table content */
    private String escapeMarkdownTableContent(String content) {
        if (content == null) {
            return "";
        }

        // Escape special characters in Markdown table
        return content.replace("|", "\\|") // Escape pipe symbol
                .replace("\n", " ") // Replace newlines with spaces
                .replace("\r", "") // Remove carriage returns
                .trim();
    }

    /** Check if it's a hardcoded default value configuration line */
    private boolean isHardcodedDefaultValue(String trimmedLine) {
        if (trimmedLine.isEmpty()
                || trimmedLine.startsWith(COMMENT_PREFIX)
                || !trimmedLine.contains(EQUALS_SIGN)) {
            return false;
        }

        // Exclude lines containing variables (these are already handled elsewhere)
        if (containsVariable(trimmedLine)) {
            return false;
        }

        // Exclude structural lines (such as "}" etc.)
        if (trimmedLine.equals(CLOSE_BRACE) || trimmedLine.equals(OPEN_BRACE)) {
            return false;
        }

        // General pattern: any key = value configuration line that doesn't contain variables is
        // considered a hardcoded default value
        // This includes: numbers, booleans, quoted strings, etc.
        return trimmedLine.matches(".*=\\s*(.+)\\s*$");
    }

    /** Record hardcoded default value */
    private void recordHardcodedDefaultValue(String trimmedLine, String targetContext) {
        if (mappingTracker == null) {
            return;
        }

        // Extract configuration key and value
        String[] parts = trimmedLine.split(EQUALS_SIGN, 2);
        if (parts.length != 2) {
            return;
        }

        String key = parts[0].trim();
        String value = parts[1].trim();

        // Remove quotes
        if (value.startsWith(QUOTE_DOUBLE) && value.endsWith(QUOTE_DOUBLE)) {
            value = value.substring(1, value.length() - 1);
        }

        // Record as default value
        mappingTracker.recordDefaultValue(targetContext, value, "Template hardcoded default value");

        logger.debug(
                "Record hardcoded default value: {} = {} (path: {})", key, value, targetContext);
    }

    /**
     * Smart context parsing - analyze template structure line by line, infer accurate target field
     * paths
     */
    private String resolveWithSmartContext(String content, JsonNode rootNode) {
        StringBuilder result = new StringBuilder();
        String[] lines = content.split("\n");
        List<String> configPath = new ArrayList<>(); // Current configuration path stack

        for (String line : lines) {
            String trimmedLine = line.trim();
            int indentLevel = getIndentLevel(line);

            // Update configuration path stack
            updateConfigPath(configPath, trimmedLine, indentLevel);

            if (containsVariable(line)) {
                String resolvedLine = processVariableLine(line, trimmedLine, configPath, rootNode);
                result.append(resolvedLine).append("\n");
            } else {
                processNonVariableLine(line, trimmedLine, configPath);
                result.append(line).append("\n");
            }
        }

        return removeTrailingNewline(result);
    }

    /**
     * Process lines containing variables
     *
     * @param line original line
     * @param trimmedLine trimmed line
     * @param configPath configuration path stack
     * @param rootNode JSON root node
     * @return parsed line
     */
    private String processVariableLine(
            String line, String trimmedLine, List<String> configPath, JsonNode rootNode) {
        logger.debug("Found line containing variables: {}", trimmedLine);
        String targetContext = buildTargetContext(configPath, trimmedLine);
        String previousContext = this.currentTargetContext;
        this.currentTargetContext = targetContext;

        try {
            boolean hasFilters = containsFilters(line);
            String originalLine = line;

            // Check if it's a real complex transformation (multiple variables or complex
            // expressions)
            boolean isComplexTransform = hasFilters && isReallyComplexTransform(line);

            // Only set complex transformation flag for truly complex transformations
            if (isComplexTransform) {
                processingComplexTransform = true;
            }

            // Parse variables in this line
            String resolvedLine = resolveJinja2FilterVariables(line, rootNode);
            resolvedLine = resolveJinja2Variables(resolvedLine, rootNode);

            // Only record as complex transformation mapping for truly complex transformations
            if (isComplexTransform && mappingTracker != null) {
                recordComplexTransformMapping(originalLine, resolvedLine, targetContext);
            }

            return resolvedLine;
        } finally {
            // Restore previous context and flags
            this.currentTargetContext = previousContext;
            this.processingComplexTransform = false;
        }
    }

    /**
     * Process lines not containing variables
     *
     * @param line original line
     * @param trimmedLine trimmed line
     * @param configPath configuration path stack
     */
    private void processNonVariableLine(String line, String trimmedLine, List<String> configPath) {
        // Check if it's a hardcoded default value configuration line
        if (isHardcodedDefaultValue(trimmedLine)) {
            String targetContext = buildTargetContext(configPath, trimmedLine);
            recordHardcodedDefaultValue(trimmedLine, targetContext);
        }
    }

    /**
     * Remove trailing newline from result
     *
     * @param result string builder
     * @return processed string
     */
    private String removeTrailingNewline(StringBuilder result) {
        if (result.length() > 0) {
            result.setLength(result.length() - 1);
        }
        return result.toString();
    }

    /** Check if line contains template variables */
    private boolean containsVariable(String line) {
        return line.contains(TEMPLATE_VAR_START) && line.contains(TEMPLATE_VAR_END);
    }

    /** Get indentation level of line */
    private int getIndentLevel(String line) {
        int indent = 0;
        for (char c : line.toCharArray()) {
            if (c == ' ') {
                indent++;
            } else if (c == '\t') {
                indent += TAB_SIZE; // tab is considered as TAB_SIZE spaces
            } else {
                break;
            }
        }
        return indent;
    }

    /** Update configuration path stack */
    private void updateConfigPath(List<String> configPath, String trimmedLine, int indentLevel) {
        logger.debug(
                "Update configuration path: indentLevel={}, current configPath={}, trimmedLine='{}'",
                indentLevel,
                configPath,
                trimmedLine);

        // Ignore empty lines and comment lines, don't let them affect configuration path
        if (trimmedLine.isEmpty() || trimmedLine.startsWith(COMMENT_PREFIX)) {
            logger.debug(
                    "Ignore empty line or comment line, keep configPath unchanged: {}", configPath);
            return;
        }

        // Adjust path depth based on indentation (every INDENT_SIZE spaces is one level)
        int targetDepth = indentLevel / INDENT_SIZE;

        logger.debug("Calculate target depth: targetDepth={}", targetDepth);

        while (configPath.size() > targetDepth) {
            String removed = configPath.remove(configPath.size() - 1);
            logger.debug("Remove path element: {}, remaining configPath={}", removed, configPath);
        }

        // If this is the start of a configuration block, add to path
        if (trimmedLine.endsWith(OPEN_BRACE)) {
            String configKey = trimmedLine.substring(0, trimmedLine.indexOf(OPEN_BRACE)).trim();
            if (!configKey.isEmpty()) {
                configPath.add(configKey);
                logger.debug("Add path element: {}, updated configPath={}", configKey, configPath);
            }
        }
    }

    /** Build target context path */
    private String buildTargetContext(List<String> configPath, String trimmedLine) {
        StringBuilder targetPath = new StringBuilder();

        // Add configuration path
        for (String pathPart : configPath) {
            if (targetPath.length() > 0) {
                targetPath.append(".");
            }
            targetPath.append(pathPart);
        }

        // If current line contains specific configuration item (key = value format), add
        // configuration key
        if (trimmedLine.contains(EQUALS_SIGN)) {
            String configKey = extractConfigKey(trimmedLine);
            if (configKey != null && !configKey.isEmpty()) {
                if (targetPath.length() > 0) {
                    targetPath.append(".");
                }
                targetPath.append(configKey);
            }
        }

        String result = targetPath.toString();
        logger.debug(
                "Build target context: configPath={}, trimmedLine='{}', result='{}'",
                configPath,
                trimmedLine,
                result);
        return result;
    }

    /** Extract configuration key name */
    private String extractConfigKey(String trimmedLine) {
        if (trimmedLine.contains("=")) {
            // key = value format
            return trimmedLine.substring(0, trimmedLine.indexOf(EQUALS_SIGN)).trim();
        }
        return null;
    }

    /**
     * Analyze template and extract field mapping relationships (alternative to HOCON parsing)
     *
     * @param templateContent template content
     * @param templateType template type (source/sink)
     * @return mapping from field paths to variable lists
     */
    public Map<String, List<String>> analyzeTemplateFieldMappings(
            String templateContent, String templateType) {
        Map<String, List<String>> fieldMappings = new HashMap<>();

        if (templateContent == null || templateContent.trim().isEmpty()) {
            return fieldMappings;
        }

        String[] lines = templateContent.split("\n");
        List<String> configPath = new ArrayList<>();

        for (String line : lines) {
            String trimmedLine = line.trim();
            int indentLevel = getIndentLevel(line);

            // Update configuration path stack
            updateConfigPath(configPath, trimmedLine, indentLevel);

            // If this line contains variables, extract field path and variables
            if (containsVariable(line)) {
                String fieldPath = buildFieldPath(templateType, configPath, trimmedLine);
                List<String> variables = extractVariablesFromLine(line);

                if (!variables.isEmpty()) {
                    fieldMappings.put(fieldPath, variables);
                    logger.debug("Extract field mapping: {} -> {}", fieldPath, variables);
                }
            }
        }

        return fieldMappings;
    }

    /** Extract all template variables from line */
    private List<String> extractVariablesFromLine(String line) {
        List<String> variables = new ArrayList<>();

        // Extract filter variables
        Matcher filterMatcher = JINJA2_FILTER_PATTERN.matcher(line);
        while (filterMatcher.find()) {
            String path = filterMatcher.group(1).trim();
            variables.add(path);
        }

        // Extract basic variables (excluding those already matched by filter pattern)
        String lineAfterFilters = filterMatcher.replaceAll("");
        Matcher variableMatcher = JINJA2_VARIABLE_PATTERN.matcher(lineAfterFilters);
        while (variableMatcher.find()) {
            String path = variableMatcher.group(1).trim();
            variables.add(path);
        }

        return variables;
    }

    /** Build field path */
    private String buildFieldPath(
            String templateType, List<String> configPath, String trimmedLine) {
        StringBuilder fieldPath = new StringBuilder();

        // Add template type prefix
        if (templateType != null && !templateType.isEmpty()) {
            fieldPath.append(templateType);
        }

        // Add configuration path
        for (String pathPart : configPath) {
            if (fieldPath.length() > 0) {
                fieldPath.append(".");
            }
            fieldPath.append(pathPart);
        }

        // If current line contains specific configuration item (key = value format), add
        // configuration key
        String configKey = extractConfigKey(trimmedLine);
        if (configKey != null && !configKey.isEmpty()) {
            if (fieldPath.length() > 0) {
                fieldPath.append(".");
            }
            fieldPath.append(configKey);
        }

        return fieldPath.toString();
    }

    /**
     * Use template analysis to parse template and track field mappings (using raw JSON string)
     *
     * @param templateContent template content
     * @param templateType template type (source/sink)
     * @param dataXJsonContent DataX JSON configuration content
     * @return parsed content
     */
    public String resolveWithTemplateAnalysis(
            String templateContent, String templateType, String dataXJsonContent) {
        if (templateContent == null || templateContent.trim().isEmpty()) {
            return templateContent;
        }

        logger.info("Using template analysis to parse template type: {}", templateType);

        try {
            // 1. Analyze template, extract field variable mappings
            Map<String, List<String>> fieldVariables =
                    analyzeTemplateFieldMappings(templateContent, templateType);

            // 2. Parse JSON string directly to JsonNode
            JsonNode rootNode = objectMapper.readTree(dataXJsonContent);

            // 3. Use smart context parsing to handle all variables
            String result = resolveWithSmartContext(templateContent, rootNode);

            logger.info(LOG_MSG_TEMPLATE_ANALYSIS_COMPLETE, fieldVariables.size());
            return result;

        } catch (Exception e) {
            handleTemplateException(ERROR_MSG_TEMPLATE_ANALYSIS_FAILED, e);
            return null; // This line won't execute, but compiler needs it
        }
    }

    /** Validate template syntax (based on Jinja2 pattern) */
    public boolean validateTemplate(String templateContent) {
        if (templateContent == null || templateContent.trim().isEmpty()) {
            return true;
        }

        try {
            // Check for unclosed template variables
            long openCount = templateContent.chars().filter(ch -> ch == '{').count();
            long closeCount = templateContent.chars().filter(ch -> ch == '}').count();

            if (openCount != closeCount) {
                logger.warn("Template validation failed: mismatched braces");
                return false;
            }

            // Check if variable syntax is correct
            Matcher matcher = JINJA2_VARIABLE_PATTERN.matcher(templateContent);
            while (matcher.find()) {
                String variable = matcher.group(1).trim();
                if (variable.isEmpty()) {
                    logger.warn("Template validation failed: found empty variable");
                    return false;
                }
            }

            Matcher filterMatcher = JINJA2_FILTER_PATTERN.matcher(templateContent);
            while (filterMatcher.find()) {
                String variable = filterMatcher.group(1).trim();
                String filter = filterMatcher.group(2).trim();
                if (variable.isEmpty() || filter.isEmpty()) {
                    logger.warn("Template validation failed: found empty variable or filter");
                    return false;
                }
            }

            return true;
        } catch (Exception e) {
            logger.error("Template validation exception: {}", e.getMessage(), e);
            return false;
        }
    }
}
