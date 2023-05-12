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

package org.apache.seatunnel.api.configuration;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("checkstyle:StaticVariableName")
public class ReadableConfigTest {
    private static final String CONFIG_PATH = "/conf/option-test.conf";
    private static ReadonlyConfig config;
    private static Map<String, String> map;

    @BeforeAll
    public static void prepare() throws URISyntaxException {
        Config rawConfig =
                ConfigFactory.parseFile(
                                Paths.get(ReadableConfigTest.class.getResource(CONFIG_PATH).toURI())
                                        .toFile())
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        config = ReadonlyConfig.fromConfig(rawConfig.getConfigList("source").get(0));
        map = new HashMap<>();
        map.put("inner.path", "mac");
        map.put("inner.name", "ashulin");
        map.put("inner.map", "{\"fantasy\":\"final\"}");
        map.put("type", "source");
        map.put("patch.note", "hollow");
        map.put("name", "saitou");
    }

    @Test
    public void testBooleanOption() {
        Assertions.assertEquals(
                true, config.get(Options.key("option.bool").booleanType().noDefaultValue()));
        Assertions.assertEquals(
                false, config.get(Options.key("option.bool-str").booleanType().noDefaultValue()));
        Assertions.assertEquals(
                true, config.get(Options.key("option.int-str").booleanType().noDefaultValue()));
        Assertions.assertNull(
                config.get(Options.key("option.not-exist").booleanType().noDefaultValue()));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> config.get(Options.key("option.string").booleanType().noDefaultValue()));
    }

    @Test
    public void testIntOption() {
        Assertions.assertEquals(
                2147483647, config.get(Options.key("option.int").intType().noDefaultValue()));
        Assertions.assertEquals(
                100, config.get(Options.key("option.int-str").intType().noDefaultValue()));
        Assertions.assertEquals(
                2147483647,
                config.get(Options.key("option.not-exist").intType().defaultValue(2147483647)));
        Assertions.assertNull(
                config.get(Options.key("option.not-exist").intType().noDefaultValue()));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> config.get(Options.key("option.long").intType().noDefaultValue()));
    }

    @Test
    public void testLongOption() {
        Assertions.assertEquals(
                21474836470L, config.get(Options.key("option.long").longType().noDefaultValue()));
        Assertions.assertEquals(
                21474836470L,
                config.get(Options.key("option.long-str").longType().noDefaultValue()));
        Assertions.assertNull(
                config.get(Options.key("option.not-exist").longType().noDefaultValue()));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> config.get(Options.key("option.bool").intType().noDefaultValue()));
    }

    @Test
    public void testFloatOption() {
        Assertions.assertEquals(
                3.3333F, config.get(Options.key("option.float").floatType().noDefaultValue()));
        Assertions.assertEquals(
                21474836470F,
                config.get(Options.key("option.long-str").floatType().noDefaultValue()));
        Assertions.assertEquals(
                3.1415F, config.get(Options.key("option.float-str").floatType().noDefaultValue()));
        Assertions.assertNull(
                config.get(Options.key("option.not-exist").floatType().noDefaultValue()));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> config.get(Options.key("option.bool-str").floatType().noDefaultValue()));
    }

    @Test
    public void testDoubleOption() {
        Assertions.assertEquals(
                3.1415926535897932384626433832795028841971D,
                config.get(Options.key("option.double").doubleType().noDefaultValue()));
        Assertions.assertEquals(
                3.1415926535897932384626433832795028841971D,
                config.get(Options.key("option.double-str").doubleType().noDefaultValue()));
        Assertions.assertEquals(
                21474836470D,
                config.get(Options.key("option.long-str").doubleType().noDefaultValue()));
        Assertions.assertEquals(
                3.1415D, config.get(Options.key("option.float-str").doubleType().noDefaultValue()));
        Assertions.assertNull(
                config.get(Options.key("option.not-exist").doubleType().noDefaultValue()));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> config.get(Options.key("option.bool-str").doubleType().noDefaultValue()));
    }

    @Test
    public void testStringOption() {
        Assertions.assertEquals(
                "Hello, Apache SeaTunnel",
                config.get(Options.key("option.string").stringType().noDefaultValue()));
        // 'option.double' is not represented as a string and is expected to lose precision
        Assertions.assertNotEquals(
                "3.1415926535897932384626433832795028841971",
                config.get(Options.key("option.double").stringType().noDefaultValue()));
        Assertions.assertEquals(
                "3.1415926535897932384626433832795028841971",
                config.get(Options.key("option.double-str").stringType().noDefaultValue()));
        Assertions.assertNull(
                config.get(Options.key("option.not-exist").stringType().noDefaultValue()));
    }

    @Test
    public void testEnumOption() {
        Assertions.assertEquals(
                OptionTest.TestMode.LATEST,
                config.get(
                        Options.key("option.enum")
                                .enumType(OptionTest.TestMode.class)
                                .noDefaultValue()));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        config.get(
                                Options.key("option.string")
                                        .enumType(OptionTest.TestMode.class)
                                        .noDefaultValue()));
        Assertions.assertNull(
                config.get(
                        Options.key("option.not-exist")
                                .enumType(OptionTest.TestMode.class)
                                .noDefaultValue()));
    }

    @Test
    public void testBasicMapOption() {
        Assertions.assertEquals(
                map, config.get(Options.key("option.map").mapType().noDefaultValue()));
        Map<String, String> newMap = new HashMap<>();
        newMap.put("fantasy", "final");
        Assertions.assertEquals(
                newMap, config.get(Options.key("option.map.inner.map").mapType().noDefaultValue()));
        Assertions.assertTrue(
                StringUtils.isNotBlank(
                        config.get(Options.key("option").stringType().noDefaultValue())));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> config.get(Options.key("option.string").mapType().noDefaultValue()));
        Assertions.assertNull(
                config.get(
                        Options.key("option.not-exist")
                                .enumType(OptionTest.TestMode.class)
                                .noDefaultValue()));
    }

    @Test
    public void testBasicListOption() {
        List<String> list = new ArrayList<>();
        list.add("Hello");
        list.add("Apache SeaTunnel");
        Assertions.assertEquals(
                list, config.get(Options.key("option.list-json").listType().noDefaultValue()));
        list = new ArrayList<>();
        list.add("final");
        list.add("fantasy");
        list.add("VII");
        Assertions.assertEquals(
                list, config.get(Options.key("option.list").listType().noDefaultValue()));
        list = new ArrayList<>();
        list.add("Silk");
        list.add("Song");
        Assertions.assertEquals(
                list, config.get(Options.key("option.list-str").listType().noDefaultValue()));
    }

    @Test
    public void testComplexTypeOption() {
        List<Map<String, List<Map<String, String>>>> complexType =
                config.get(
                        Options.key("option.complex-type")
                                .type(
                                        new TypeReference<
                                                List<Map<String, List<Map<String, String>>>>>() {})
                                .noDefaultValue());
        Assertions.assertEquals(1, complexType.size());
        Assertions.assertEquals(2, complexType.get(0).size());
        complexType
                .get(0)
                .values()
                .forEach(
                        value -> {
                            Assertions.assertEquals(1, value.size());
                            Assertions.assertEquals(map, value.get(0));
                        });
    }

    @Test
    public void testEnumListOption() {
        List<OptionTest.TestMode> list = new ArrayList<>();
        list.add(OptionTest.TestMode.EARLIEST);
        list.add(OptionTest.TestMode.LATEST);
        Assertions.assertEquals(
                list,
                config.get(
                        Options.key("option.enum-list")
                                .listType(OptionTest.TestMode.class)
                                .noDefaultValue()));
    }

    @Test
    public void testNumericListOption() {
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        Assertions.assertEquals(
                list,
                config.get(
                        Options.key("option.numeric-list")
                                .listType(Integer.class)
                                .noDefaultValue()));
        List<Long> list2 = new ArrayList<>();
        list2.add(1L);
        list2.add(2L);
        Assertions.assertEquals(
                list2,
                config.get(
                        Options.key("option.numeric-list").listType(Long.class).noDefaultValue()));
        List<Double> list3 = new ArrayList<>();
        list3.add(1D);
        list3.add(2D);
        Assertions.assertEquals(
                list3,
                config.get(
                        Options.key("option.numeric-list")
                                .listType(Double.class)
                                .noDefaultValue()));
    }

    @Test
    public void testFallbackKey() {
        Map<String, Object> map = new HashMap<>();
        map.put("user", "ashulin");
        final Option<String> usernameOption =
                Options.key("username").stringType().noDefaultValue().withFallbackKeys("user");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(map);
        Assertions.assertEquals("ashulin", readonlyConfig.get(usernameOption));
        Assertions.assertNull(
                readonlyConfig.get(Options.key("username").stringType().noDefaultValue()));
        map.put("username", "ark");
        readonlyConfig = ReadonlyConfig.fromMap(map);
        Assertions.assertEquals("ark", readonlyConfig.get(usernameOption));
    }
}
