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

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HiveMetaStoreCatalogKerberosRenewTest {

    private static void set(Object target, String field, Object value) throws Exception {
        Field f = null;
        Class<?> cls = target.getClass();
        // Fields are declared on HiveMetaStoreCatalog; if a subclass instance is passed, climb up
        while (cls != null) {
            try {
                f = cls.getDeclaredField(field);
                break;
            } catch (NoSuchFieldException ignore) {
                cls = cls.getSuperclass();
            }
        }
        if (f == null) {
            throw new NoSuchFieldException(field);
        }
        f.setAccessible(true);
        f.set(target, value);
    }

    private static Object invoke(Object target, String method) throws Exception {
        Method m = null;
        Class<?> cls = target.getClass();
        while (cls != null) {
            try {
                m = cls.getDeclaredMethod(method);
                break;
            } catch (NoSuchMethodException ignore) {
                cls = cls.getSuperclass();
            }
        }
        if (m == null) {
            throw new NoSuchMethodException(method);
        }
        m.setAccessible(true);
        return m.invoke(target);
    }

    @Test
    void testGetClientTriggersMaybeReloginFromKeytab() throws Exception {
        ReadonlyConfig cfg = Mockito.mock(ReadonlyConfig.class);
        HiveMetaStoreCatalog catalog = new HiveMetaStoreCatalog(cfg);

        HiveMetaStoreClient client = Mockito.mock(HiveMetaStoreClient.class);
        UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
        when(ugi.isFromKeytab()).thenReturn(true);

        set(catalog, "hiveClient", client);
        set(catalog, "userGroupInformation", ugi);
        set(catalog, "kerberosEnabled", true);

        HiveMetaStoreClient out = (HiveMetaStoreClient) invoke(catalog, "getClient");
        Assertions.assertNotNull(out);
        verify(ugi, times(1)).checkTGTAndReloginFromKeytab();
    }

    @Test
    void testGetClientTriggersMaybeReloginNotFromKeytab() throws Exception {
        ReadonlyConfig cfg = Mockito.mock(ReadonlyConfig.class);
        HiveMetaStoreCatalog catalog = new HiveMetaStoreCatalog(cfg);

        HiveMetaStoreClient client = Mockito.mock(HiveMetaStoreClient.class);
        UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
        when(ugi.isFromKeytab()).thenReturn(false);

        set(catalog, "hiveClient", client);
        set(catalog, "userGroupInformation", ugi);
        set(catalog, "kerberosEnabled", true);

        HiveMetaStoreClient out = (HiveMetaStoreClient) invoke(catalog, "getClient");
        Assertions.assertNotNull(out);
        verify(ugi, never()).checkTGTAndReloginFromKeytab();
    }

    @Test
    void testGetClientReloginThrowsSwallowed() throws Exception {
        ReadonlyConfig cfg = Mockito.mock(ReadonlyConfig.class);
        HiveMetaStoreCatalog catalog = new HiveMetaStoreCatalog(cfg);

        HiveMetaStoreClient client = Mockito.mock(HiveMetaStoreClient.class);
        UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
        when(ugi.isFromKeytab()).thenReturn(true);
        doThrow(new RuntimeException("test")).when(ugi).checkTGTAndReloginFromKeytab();

        set(catalog, "hiveClient", client);
        set(catalog, "userGroupInformation", ugi);
        set(catalog, "kerberosEnabled", true);

        Assertions.assertDoesNotThrow(
                () -> {
                    try {
                        invoke(catalog, "getClient");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        verify(ugi, times(1)).checkTGTAndReloginFromKeytab();
    }
}
