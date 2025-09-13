package org.apache.seatunnel.api.metalake;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigList;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueType;

import org.apache.seatunnel.common.utils.PlaceholderUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class MetalakeConfigUtils {

    private static final Pattern pattern = Pattern.compile("\\$\\{[^}]*\\}");

    public static Config getMetalakeConfig(Config jobConfigTmp) {
        Config update = jobConfigTmp;
        String metalakeType = System.getenv("METALAKE_TYPE");
        String metalakeUrl = System.getenv("METALAKE_URL");

        MetalakeClient metalakeClient = MetalakeClientFactory.create(metalakeType, metalakeUrl);

        try {
            ConfigList sourceList = jobConfigTmp.getList("source");
            List<ConfigValue> newSourceList = new ArrayList<>(sourceList);

            for (int i = 0; i < sourceList.size(); i++) {
                ConfigObject sourceObj = (ConfigObject) sourceList.get(i);
                if (sourceObj.containsKey("sourceId")) {
                    ConfigObject tmp = sourceObj;
                    String sourceId = sourceObj.toConfig().getString("sourceId");
                    JsonNode metalakeJson = metalakeClient.getMetaInfo(sourceId);
                    for (Map.Entry<String, ConfigValue> entry : sourceObj.entrySet()) {
                        String subKey = entry.getKey();
                        ConfigValue value = entry.getValue();

                        if (value.valueType() == ConfigValueType.STRING) {
                            String strValue = (String) value.unwrapped();
                            Matcher matcher = pattern.matcher(strValue);
                            if (matcher.find()) {
                                String placeholder = matcher.group(1);

                                if (metalakeJson.has(placeholder)) {
                                    String replaced = metalakeJson.get(placeholder).asText();
                                    String newValue =
                                            PlaceholderUtils.replacePlaceholders(
                                                    strValue, placeholder, replaced);
                                    tmp =
                                            tmp.withValue(
                                                    subKey,
                                                    ConfigValueFactory.fromAnyRef(newValue));
                                }
                            }
                        }
                    }
                    newSourceList.set(i, tmp);
                }
            }
            update = update.withValue("source", ConfigValueFactory.fromIterable(newSourceList));
        } catch (IOException e) {
            log.error("Fail to get MetaInfo, metalakeUrl: {}", metalakeUrl, e);
        }

        try {
            ConfigList sinkList = jobConfigTmp.getList("sink");
            List<ConfigValue> newSinkList = new ArrayList<>(sinkList);

            for (int i = 0; i < sinkList.size(); i++) {
                ConfigObject sinkObj = (ConfigObject) sinkList.get(i);
                if (sinkObj.containsKey("sourceId")) {
                    ConfigObject tmp = sinkObj;
                    String sourceId = sinkObj.toConfig().getString("sourceId");
                    JsonNode metalakeJson = metalakeClient.getMetaInfo(sourceId);
                    for (Map.Entry<String, ConfigValue> entry : sinkObj.entrySet()) {
                        String subKey = entry.getKey();
                        ConfigValue value = entry.getValue();

                        if (value.valueType() == ConfigValueType.STRING) {
                            String strValue = (String) value.unwrapped();
                            Matcher matcher = pattern.matcher(strValue);
                            if (matcher.find()) {
                                String placeholder = matcher.group(1);

                                if (metalakeJson.has(placeholder)) {
                                    String replaced = metalakeJson.get(placeholder).asText();
                                    String newValue =
                                            PlaceholderUtils.replacePlaceholders(
                                                    strValue, placeholder, replaced);
                                    tmp =
                                            tmp.withValue(
                                                    subKey,
                                                    ConfigValueFactory.fromAnyRef(newValue));
                                }
                            }
                        }
                    }
                    newSinkList.set(i, tmp);
                }
            }
            update = update.withValue("sink", ConfigValueFactory.fromIterable(newSinkList));
        } catch (IOException e) {
            log.error("Fail to get MetaInfo, metalakeUrl: {}", metalakeUrl, e);
        }
        return update;
    }
}
