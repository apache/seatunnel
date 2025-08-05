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

package org.apache.seatunnel.tools.x2seatunnel.util;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/** Parse the YAML configuration file and map it to the ConversionConfig object */
public class YamlConfigParser {
    @SuppressWarnings("unchecked")
    public static ConversionConfig parse(String yamlPath) {
        try (InputStream in = Files.newInputStream(Paths.get(yamlPath))) {
            Yaml yaml = new Yaml();
            Map<String, Object> obj = yaml.load(in);
            ConversionConfig config = new ConversionConfig();
            if (obj.containsKey("source")) {
                Object s = obj.get("source");
                if (s instanceof Map) {
                    config.setSource(((Map<String, String>) s).get("path"));
                } else if (s instanceof String) {
                    config.setSource((String) s);
                }
            }
            if (obj.containsKey("target")) {
                config.setTarget((String) obj.get("target"));
            }
            if (obj.containsKey("report")) {
                config.setReport((String) obj.get("report"));
            }
            if (obj.containsKey("template")) {
                config.setTemplate((String) obj.get("template"));
            }
            if (obj.containsKey("sourceType")) {
                config.setSourceType((String) obj.get("sourceType"));
            }
            if (obj.containsKey("options")) {
                Map<String, Object> opt = (Map<String, Object>) obj.get("options");
                if (Boolean.TRUE.equals(opt.get("verbose"))) {
                    config.setVerbose(true);
                }
            }
            return config;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load YAML configuration: " + e.getMessage(), e);
        }
    }
}
