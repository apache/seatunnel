package org.apache.seatunnel.common.utils;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PlaceholderUtilsTest {

    @Test
    void testMultiplePlaceholdersWithDefault() {
        String input = "select * from ${resName:fake_test}_table where name = '${nameValForEnv}'";

        String result =
                PlaceholderUtils.processPlaceholders(
                        input,
                        key -> false,
                        new LinkedHashMap<String, String>(),
                        new LinkedHashMap<String, String>());
        assertEquals("select * from fake_test_table where name = '${nameValForEnv}'", result);
    }
}
