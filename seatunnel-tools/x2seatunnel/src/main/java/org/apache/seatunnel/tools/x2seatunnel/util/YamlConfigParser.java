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
