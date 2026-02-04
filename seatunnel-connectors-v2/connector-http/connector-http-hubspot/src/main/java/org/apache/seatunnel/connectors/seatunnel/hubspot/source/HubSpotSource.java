package org.apache.seatunnel.connectors.seatunnel.hubspot.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class HubSpotSource extends HttpSource {

    public HubSpotSource(ReadonlyConfig config) {
        // We calculate the specific HubSpot config (URL, Auth) BEFORE calling super()
        // This injects our logic into the standard HttpSource
        super(buildHubSpotConfig(config));
    }

    @Override
    public String getPluginName() {
        return "HubSpot";
    }

    /**
     * This helper method builds the final configuration that the parent HttpSource needs. It
     * converts "object_type" -> "url" and "access_token" -> "headers".
     */
    private static ReadonlyConfig buildHubSpotConfig(ReadonlyConfig originalConfig) {
        Map<String, Object> newConfigMap = new HashMap<>(originalConfig.toMap());

        // 1. Get user inputs
        String objectType =
                originalConfig.getOptional(HubSpotSourceFactory.OBJECT_TYPE).orElse("contacts");
        String accessToken = originalConfig.get(HubSpotSourceFactory.ACCESS_TOKEN);

        // 2. Generate the URL dynamically
        String finalUrl = "https://api.hubapi.com/crm/v3/objects/" + objectType;
        newConfigMap.put("url", finalUrl);

        // 3. Inject Authentication Headers
        // The HttpSource expects a map under the key "headers"
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + accessToken);
        headers.put("Content-Type", "application/json");
        newConfigMap.put("headers", headers);

        // 4. Force the content field to "results" (so it parses the list correctly)
        newConfigMap.put("content_field", "results");

        // 5. Return the new merged config
        return ReadonlyConfig.fromMap(newConfigMap);
    }
}
