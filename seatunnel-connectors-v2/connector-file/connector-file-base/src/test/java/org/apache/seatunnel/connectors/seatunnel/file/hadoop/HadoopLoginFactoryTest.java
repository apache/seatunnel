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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class HadoopLoginFactoryTest {

    private MiniKdc miniKdc;

    private File workDir;

    @BeforeEach
    public void startMiniKdc() throws Exception {
        workDir = new File(System.getProperty("test.dir", "target"));
        miniKdc = new MiniKdc(MiniKdc.createConf(), workDir);
        miniKdc.start();
    }

    @AfterEach
    public void stopMiniKdc() {
        if (miniKdc != null) {
            miniKdc.stop();
        }
    }

    @Test
    void loginWithKerberos_success() throws Exception {
        miniKdc.createPrincipal(new File(workDir, "tom.keytab"), "tom");

        UserGroupInformation userGroupInformation =
                HadoopLoginFactory.loginWithKerberos(
                        createConfiguration(),
                        null,
                        "tom",
                        workDir.getPath() + "/" + "tom.keytab",
                        (conf, ugi) -> ugi);

        assertNotNull(userGroupInformation);
        assertEquals("tom@EXAMPLE.COM", userGroupInformation.getUserName());
    }

    @Test
    void loginWithKerberos_multiple_times() throws Exception {
        miniKdc.createPrincipal(new File(workDir, "tom1.keytab"), "tom1");
        miniKdc.createPrincipal(new File(workDir, "tom2.keytab"), "tom2");

        UserGroupInformation tom1 =
                HadoopLoginFactory.loginWithKerberos(
                        createConfiguration(),
                        null,
                        "tom1",
                        workDir.getPath() + "/" + "tom1.keytab",
                        (conf, ugi) -> ugi);

        assertNotNull(tom1);
        assertEquals("tom1@EXAMPLE.COM", tom1.getUserName());

        UserGroupInformation tom2 =
                HadoopLoginFactory.loginWithKerberos(
                        createConfiguration(),
                        null,
                        "tom2",
                        workDir.getPath() + "/" + "tom2.keytab",
                        (conf, ugi) -> ugi);

        assertNotNull(tom2);
        assertEquals("tom2@EXAMPLE.COM", tom2.getUserName());
    }

    @Test
    void loginWithKerberos_fail() {
        Assertions.assertThrows(
                Exception.class,
                () ->
                        HadoopLoginFactory.loginWithKerberos(
                                createConfiguration(),
                                null,
                                "tom",
                                workDir.getPath() + "/" + "tom.keytab",
                                (conf, ugi) -> ugi));
    }

    @Test
    void loginWithBadConfiguration() {
        IllegalArgumentException illegalArgumentException =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                HadoopLoginFactory.loginWithKerberos(
                                        new Configuration(),
                                        null,
                                        "tom",
                                        workDir.getPath() + "/" + "tom.keytab",
                                        (conf, ugi) -> ugi));
        Assertions.assertEquals(
                "hadoop.security.authentication must be kerberos",
                illegalArgumentException.getMessage());
    }

    private Configuration createConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication", "kerberos");
        return configuration;
    }
}
