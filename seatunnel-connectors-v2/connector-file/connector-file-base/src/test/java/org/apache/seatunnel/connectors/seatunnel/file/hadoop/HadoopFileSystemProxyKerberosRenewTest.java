package org.apache.seatunnel.connectors.seatunnel.file.hadoop;

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.hadoop.security.UserGroupInformation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

static class HadoopFileSystemProxyKerberosRenewTest {

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

    @Test
    void testMaybeRelogin_fromKeytab_callsCheck() throws Exception {
        HadoopConf conf = new HadoopConf("file:///");
        HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(conf);

        UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
        when(ugi.isFromKeytab()).thenReturn(true);

        set(proxy, "isAuthTypeKerberos", true);
        set(proxy, "userGroupInformation", ugi);

        // invoke private maybeRelogin()
        invoke(proxy, "maybeRelogin");

        verify(ugi, times(1)).checkTGTAndReloginFromKeytab();
    }

    @Test
    void testMaybeRelogin_notFromKeytab_noCheck() throws Exception {
        HadoopConf conf = new HadoopConf("file:///");
        HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(conf);

        UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
        when(ugi.isFromKeytab()).thenReturn(false);

        set(proxy, "isAuthTypeKerberos", true);
        set(proxy, "userGroupInformation", ugi);

        invoke(proxy, "maybeRelogin");

        verify(ugi, never()).checkTGTAndReloginFromKeytab();
    }

    @Test
    void testMaybeRelogin_checkThrows_swallowed() throws Exception {
        HadoopConf conf = new HadoopConf("file:///");
        HadoopFileSystemProxy proxy = new HadoopFileSystemProxy(conf);

        UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
        when(ugi.isFromKeytab()).thenReturn(true);
        doThrow(new IOException("test")).when(ugi).checkTGTAndReloginFromKeytab();

        set(proxy, "isAuthTypeKerberos", true);
        set(proxy, "userGroupInformation", ugi);

        // should not throw out
        Assertions.assertDoesNotThrow(
                () -> {
                    try {
                        invoke(proxy, "maybeRelogin");
                    } catch (Exception e) {
                        // unwrap reflection InvocationTargetException if any
                        throw new RuntimeException(e);
                    }
                });

        verify(ugi, times(1)).checkTGTAndReloginFromKeytab();
    }
}
