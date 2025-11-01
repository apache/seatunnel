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
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveOptions;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HiveMetaStoreProxyKerberosRenewTest {

    private static void set(Object target, String field, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static Object invoke(Object target, String method) throws Exception {
        Method m = target.getClass().getDeclaredMethod(method);
        m.setAccessible(true);
        return m.invoke(target);
    }

    private ReadonlyConfig createKerberosEnabledConfig() {
        ReadonlyConfig cfg = Mockito.mock(ReadonlyConfig.class);
        when(cfg.get(HiveOptions.METASTORE_URI)).thenReturn("thrift://localhost:9083");
        when(cfg.get(HiveConfig.HADOOP_CONF_PATH)).thenReturn(null);
        when(cfg.get(HiveConfig.HIVE_SITE_PATH)).thenReturn(null);
        when(cfg.getOptional(FileBaseSourceOptions.KERBEROS_PRINCIPAL))
                .thenReturn(Optional.of("user@REALM"));
        when(cfg.getOptional(FileBaseSourceOptions.KERBEROS_KEYTAB_PATH))
                .thenReturn(Optional.of("/path/to/keytab"));
        when(cfg.get(FileBaseSourceOptions.KRB5_PATH)).thenReturn(null);
        when(cfg.get(FileBaseSourceOptions.KERBEROS_PRINCIPAL)).thenReturn("user@REALM");
        when(cfg.get(FileBaseSourceOptions.KERBEROS_KEYTAB_PATH)).thenReturn("/path/to/keytab");
        when(cfg.get(FileBaseSourceOptions.REMOTE_USER)).thenReturn(null);
        when(cfg.getOptional(FileBaseSourceOptions.REMOTE_USER)).thenReturn(Optional.empty());
        return cfg;
    }

    @Test
    void testGetClientTriggersMaybeReloginFromKeytab() throws Exception {
        ReadonlyConfig cfg = createKerberosEnabledConfig();
        HiveMetaStoreProxy proxy = new HiveMetaStoreProxy(cfg);

        HiveMetaStoreClient client = Mockito.mock(HiveMetaStoreClient.class);
        UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
        when(ugi.isFromKeytab()).thenReturn(true);

        set(proxy, "hiveClient", client);
        set(proxy, "userGroupInformation", ugi);

        HiveMetaStoreClient out = (HiveMetaStoreClient) invoke(proxy, "getClient");
        Assertions.assertNotNull(out);
        verify(ugi, times(1)).checkTGTAndReloginFromKeytab();
    }

    @Test
    void testGetClientTriggersMaybeReloginNotFromKeytab() throws Exception {
        ReadonlyConfig cfg = createKerberosEnabledConfig();
        HiveMetaStoreProxy proxy = new HiveMetaStoreProxy(cfg);

        HiveMetaStoreClient client = Mockito.mock(HiveMetaStoreClient.class);
        UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
        when(ugi.isFromKeytab()).thenReturn(false);

        set(proxy, "hiveClient", client);
        set(proxy, "userGroupInformation", ugi);

        HiveMetaStoreClient out = (HiveMetaStoreClient) invoke(proxy, "getClient");
        Assertions.assertNotNull(out);
        verify(ugi, never()).checkTGTAndReloginFromKeytab();
    }

    @Test
    void testGetClientReloginThrowsSwallowed() throws Exception {
        ReadonlyConfig cfg = createKerberosEnabledConfig();
        HiveMetaStoreProxy proxy = new HiveMetaStoreProxy(cfg);

        HiveMetaStoreClient client = Mockito.mock(HiveMetaStoreClient.class);
        UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
        when(ugi.isFromKeytab()).thenReturn(true);
        doThrow(new RuntimeException("test")).when(ugi).checkTGTAndReloginFromKeytab();

        set(proxy, "hiveClient", client);
        set(proxy, "userGroupInformation", ugi);

        Assertions.assertDoesNotThrow(
                () -> {
                    try {
                        invoke(proxy, "getClient");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        verify(ugi, times(1)).checkTGTAndReloginFromKeytab();
    }
}
