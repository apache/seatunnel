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
package org.apache.seatunnel.connectors.seatunnel.file.hadoop;

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.hadoop.security.UserGroupInformation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HadoopFileSystemProxyKerberosRenewTest {

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
    void testMaybeReloginFromKeytabCallsCheck() throws Exception {
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
    void testMaybeReloginNotFromKeytabNoCheck() throws Exception {
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
    void testMaybeReloginCheckThrowsSwallowed() throws Exception {
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
