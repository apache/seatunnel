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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveBaseOptions;

import org.apache.hadoop.hive.conf.HiveConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.mockito.Mockito.when;

class HiveMetaStoreCatalogMetastoreUrisTest {

    private static Object invokeStatic(String method, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        Method m = HiveMetaStoreCatalog.class.getDeclaredMethod(method, parameterTypes);
        m.setAccessible(true);
        return m.invoke(null, args);
    }

    private static Object invoke(Object target, String method) throws Exception {
        Method m = HiveMetaStoreCatalog.class.getDeclaredMethod(method);
        m.setAccessible(true);
        return m.invoke(target);
    }

    private static void set(Object target, String field, Object value) throws Exception {
        Field f = HiveMetaStoreCatalog.class.getDeclaredField(field);
        f.setAccessible(true);
        f.set(target, value);
    }

    @Test
    void testNormalizeMetastoreUrisNullThrows() {
        InvocationTargetException ex =
                Assertions.assertThrows(
                        InvocationTargetException.class,
                        () ->
                                invokeStatic(
                                        "normalizeMetastoreUris",
                                        new Class<?>[] {String.class},
                                        (Object) null));
        Assertions.assertInstanceOf(NullPointerException.class, ex.getCause());
    }

    @Test
    void testNormalizeMetastoreUrisTrimsAndRemovesEmpty() throws Exception {
        String in = " thrift://hms-1:9083, thrift://hms-2:9083 , ,";
        String out =
                (String) invokeStatic("normalizeMetastoreUris", new Class<?>[] {String.class}, in);
        Assertions.assertEquals("thrift://hms-1:9083,thrift://hms-2:9083", out);
    }

    @Test
    void testGetFirstMetastoreUriNullThrows() {
        InvocationTargetException ex =
                Assertions.assertThrows(
                        InvocationTargetException.class,
                        () ->
                                invokeStatic(
                                        "getFirstMetastoreUri",
                                        new Class<?>[] {String.class},
                                        (Object) null));
        Assertions.assertInstanceOf(NullPointerException.class, ex.getCause());
    }

    @Test
    void testGetFirstMetastoreUriReturnsTrimmedFirst() throws Exception {
        String in = " thrift://hms-1:9083, thrift://hms-2:9083";
        String out =
                (String) invokeStatic("getFirstMetastoreUri", new Class<?>[] {String.class}, in);
        Assertions.assertEquals("thrift://hms-1:9083", out);
    }

    @Test
    void testGetFirstMetastoreUriSkipsBlankEntries() throws Exception {
        String in = " , thrift://a:9083, thrift://b:9083";
        String out =
                (String) invokeStatic("getFirstMetastoreUri", new Class<?>[] {String.class}, in);
        Assertions.assertEquals("thrift://a:9083", out);
    }

    @Test
    void testGetHiveServer2JdbcUrlDerivesFromFirstMetastoreUri() throws Exception {
        ReadonlyConfig cfg = Mockito.mock(ReadonlyConfig.class);
        when(cfg.get(HiveBaseOptions.METASTORE_URI))
                .thenReturn(" thrift://namenode001:9084, thrift://namenode001:9083");
        HiveMetaStoreCatalog catalog = new HiveMetaStoreCatalog(cfg);
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.server2.jdbc.url", "");
        set(catalog, "hiveConf", hiveConf);

        String jdbcUrl = (String) invoke(catalog, "getHiveServer2JdbcUrl");
        Assertions.assertEquals("jdbc:hive2://namenode001:10000/default", jdbcUrl);
    }
}
