package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;

static class HiveMetaStoreProxyKerberosRenewTest {

    private static void set(Object target, String field, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        f.set(target, value);
    }

    @Test
    void testGetClient_triggersMaybeRelogin_fromKeytab() throws Exception {
        ReadonlyConfig cfg = Mockito.mock(ReadonlyConfig.class);
        HiveMetaStoreProxy proxy = new HiveMetaStoreProxy(cfg);

        HiveMetaStoreClient client = Mockito.mock(HiveMetaStoreClient.class);
        UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
        when(ugi.isFromKeytab()).thenReturn(true);

        set(proxy, "hiveClient", client);
        set(proxy, "userGroupInformation", ugi);
        set(proxy, "kerberosEnabled", true);

        HiveMetaStoreClient out = proxy.getClient();
        Assertions.assertNotNull(out);
        verify(ugi, times(1)).checkTGTAndReloginFromKeytab();
    }

    @Test
    void testGetClient_triggersMaybeRelogin_notFromKeytab() throws Exception {
        ReadonlyConfig cfg = Mockito.mock(ReadonlyConfig.class);
        HiveMetaStoreProxy proxy = new HiveMetaStoreProxy(cfg);

        HiveMetaStoreClient client = Mockito.mock(HiveMetaStoreClient.class);
        UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
        when(ugi.isFromKeytab()).thenReturn(false);

        set(proxy, "hiveClient", client);
        set(proxy, "userGroupInformation", ugi);
        set(proxy, "kerberosEnabled", true);

        HiveMetaStoreClient out = proxy.getClient();
        Assertions.assertNotNull(out);
        verify(ugi, never()).checkTGTAndReloginFromKeytab();
    }

    @Test
    void testGetClient_reloginThrows_swallowed() throws Exception {
        ReadonlyConfig cfg = Mockito.mock(ReadonlyConfig.class);
        HiveMetaStoreProxy proxy = new HiveMetaStoreProxy(cfg);

        HiveMetaStoreClient client = Mockito.mock(HiveMetaStoreClient.class);
        UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
        when(ugi.isFromKeytab()).thenReturn(true);
        doThrow(new RuntimeException("test")).when(ugi).checkTGTAndReloginFromKeytab();

        set(proxy, "hiveClient", client);
        set(proxy, "userGroupInformation", ugi);
        set(proxy, "kerberosEnabled", true);

        Assertions.assertDoesNotThrow(proxy::getClient);
        verify(ugi, times(1)).checkTGTAndReloginFromKeytab();
    }
}
