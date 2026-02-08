package org.apache.seatunnel.connectors.seatunnel.starrocks.client;

import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.connectors.seatunnel.starrocks.client.source.StarRocksBeReadClient;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.BeHostPortMapping;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @Author li jie @Date 2026/1/7 19:12
 *
 * @desc StarRocksBeReadClientTest
 */
public class StarRocksBeReadClientTest {
    @Test
    void testBeHostPortMappingNormal() throws Exception {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        List<BeHostPortMapping> mappings =
                Arrays.asList(
                        new BeHostPortMapping("be1:9060", "192.168.1.1:31088"),
                        new BeHostPortMapping("be2:9060", "192.168.1.2:31088"));
        when(sourceConfig.getBeHostPortMapping()).thenReturn(mappings);

        Map<String, Pair<String, Integer>> result = formatBeHostPortMapping(sourceConfig);

        Pair<String, Integer> mapping = result.get("be1");
        Assertions.assertNotNull(mapping);
        Assertions.assertEquals("192.168.1.1", mapping.getKey());
        Assertions.assertEquals(31088, mapping.getValue().intValue());
    }

    @Test
    void testBeHostPortMappingNotInMapping() throws Exception {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        List<BeHostPortMapping> mappings =
                Collections.singletonList(new BeHostPortMapping("be2:9060", "192.168.1.2:31088"));
        when(sourceConfig.getBeHostPortMapping()).thenReturn(mappings);

        Map<String, Pair<String, Integer>> result = formatBeHostPortMapping(sourceConfig);

        Assertions.assertFalse(result.containsKey("be1"));
    }

    @Test
    void testBeHostPortMappingInvalidHostPortFormat() throws Exception {
        InvocationTargetException exception =
                Assertions.assertThrows(
                        InvocationTargetException.class,
                        () ->
                                invokePrivate(
                                        "extractHost",
                                        new Class[] {BeHostPortMapping.class},
                                        new BeHostPortMapping("be1", "192.168.1.1:31088")));
        Assertions.assertTrue(exception.getCause() instanceof StarRocksConnectorException);
    }

    @Test
    void testBeHostPortMappingInvalidIpPortFormat() throws Exception {
        InvocationTargetException exception =
                Assertions.assertThrows(
                        InvocationTargetException.class,
                        () ->
                                invokePrivate(
                                        "parseAccessiblePort",
                                        new Class[] {BeHostPortMapping.class},
                                        new BeHostPortMapping("be1:9060", "192.168.1.1")));
        Assertions.assertTrue(exception.getCause() instanceof StarRocksConnectorException);
    }

    @Test
    void testBeHostPortMappingPortOutOfRange() throws Exception {
        InvocationTargetException exception =
                Assertions.assertThrows(
                        InvocationTargetException.class,
                        () ->
                                invokePrivate(
                                        "parseAccessiblePort",
                                        new Class[] {BeHostPortMapping.class},
                                        new BeHostPortMapping("be1:9060", "192.168.1.1:99999")));
        Assertions.assertTrue(exception.getCause() instanceof StarRocksConnectorException);
    }

    @Test
    void testDuplicateBeHostPortMappingUsesFirst() throws Exception {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        List<BeHostPortMapping> mappings =
                Arrays.asList(
                        new BeHostPortMapping("be1:9060", "192.168.1.1:31088"),
                        new BeHostPortMapping("be1:9060", "192.168.1.2:31088"));
        when(sourceConfig.getBeHostPortMapping()).thenReturn(mappings);

        Map<String, Pair<String, Integer>> result = formatBeHostPortMapping(sourceConfig);

        Pair<String, Integer> mapping = result.get("be1");
        Assertions.assertNotNull(mapping);
        Assertions.assertEquals("192.168.1.1", mapping.getKey());
        Assertions.assertEquals(31088, mapping.getValue().intValue());
    }

    @Test
    void testBeHostPortMappingEmptyMapping() throws Exception {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        when(sourceConfig.getBeHostPortMapping()).thenReturn(Collections.emptyList());

        Map<String, Pair<String, Integer>> result = formatBeHostPortMapping(sourceConfig);

        Assertions.assertTrue(result.isEmpty());
    }

    private static Map<String, Pair<String, Integer>> formatBeHostPortMapping(
            SourceConfig sourceConfig) throws Exception {
        return (Map<String, Pair<String, Integer>>)
                invokePrivate(
                        "formatBeHostPortMapping", new Class[] {SourceConfig.class}, sourceConfig);
    }

    private static Object invokePrivate(
            String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = StarRocksBeReadClient.class.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(null, args);
    }
}
