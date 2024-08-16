package org.apache.seatunnel.connectors.seatunnel.typesense.util;

import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class URLParamsConverter {
    /**
     * Convert URL query parameters string to JSON object using Jackson ObjectMapper.
     *
     * @param paramsString URL query parameters string, e.g., "q=*&filter_by=num_employees:10"
     * @return JSON string representing the converted key-value pairs
     * @throws IllegalArgumentException if input paramsString is null or empty
     * @throws IOException              if there is an error parsing the JSON
     */
    public static String convertParamsToJson(String paramsString) {
        return Optional.ofNullable(paramsString)
                .filter(s -> !s.isEmpty())
                .map(URLParamsConverter::parseParams)
                .map(paramsMap -> {
                    try {
                        return new ObjectMapper().writeValueAsString(paramsMap);
                    } catch (IOException e) {
                        throw new RuntimeException("Error converting params to JSON", e);
                    }
                })
                .orElseThrow(() -> new IllegalArgumentException("Parameter string must not be null or empty."));
    }

    /**
     * Parse URL query parameters into a Map.
     *
     * @param paramsString URL query parameters string
     * @return Map representing the parsed key-value pairs
     * @throws IllegalArgumentException if input paramsString is null or empty
     */
    private static Map<String, String> parseParams(String paramsString) {
        return Arrays.stream(Optional.ofNullable(paramsString)
                        .filter(s -> !s.isEmpty())
                        .orElseThrow(() -> new IllegalArgumentException("Parameter string must not be null or empty."))
                        .split("&"))
                .map(part -> part.split("=", 2))
                .peek(keyValue -> {
                    if (keyValue.length != 2) {
                        throw new TypesenseConnectorException(TypesenseConnectorErrorCode.QUERY_PARAM_ERROR,
                                "Query parameter error: " + Arrays.toString(keyValue));
                    }
                })
                .collect(Collectors.toMap(keyValue -> keyValue[0], keyValue -> keyValue[1]));
    }
}
