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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.util;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AklessAccount;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;

import java.util.HashMap;
import java.util.Map;

public class MaxcomputeUtilTest {

    // --- getAccount() tests ---

    /**
     * When only accessId + accessKey are supplied, the connector must use a standard AliyunAccount
     * (long-lived static AK/SK).
     */
    @Test
    void testGetAccountReturnsAliyunAccountWhenAkSkProvided() {
        Map<String, Object> config = new HashMap<>();
        config.put("accessId", "my-id");
        config.put("accesskey", "my-key");

        Account account = MaxcomputeUtil.getAccount(ReadonlyConfig.fromMap(config));

        Assertions.assertInstanceOf(AliyunAccount.class, account);
    }

    /**
     * When accessId + accessKey + sts_token are all supplied, the connector must use an StsAccount
     * (temporary credential).
     */
    @Test
    void testGetAccountReturnsStsAccountWhenAllThreeProvided() {
        Map<String, Object> config = new HashMap<>();
        config.put("accessId", "my-id");
        config.put("accesskey", "my-key");
        config.put("sts_token", "my-sts-token");

        Account account = MaxcomputeUtil.getAccount(ReadonlyConfig.fromMap(config));

        Assertions.assertInstanceOf(StsAccount.class, account);
    }

    /**
     * When none of the access credentials are supplied, the connector must fall back to the
     * DefaultCredentialsProvider chain (AklessAccount), enabling passwordless auth via ECS RAM
     * roles, environment variables, etc.
     */
    @Test
    void testGetAccountReturnsAklessAccountWhenNothingProvided() {
        Account account = MaxcomputeUtil.getAccount(ReadonlyConfig.fromMap(new HashMap<>()));

        Assertions.assertInstanceOf(AklessAccount.class, account);
    }

    /**
     * Providing sts_token WITHOUT accessId/accessKey must fail fast with an explicit
     * IllegalArgumentException instead of letting a NullPointerException surface at runtime inside
     * the ODPS SDK.
     */
    @Test
    void testGetAccountThrowsWhenStsTokenProvidedWithoutAkSk() {
        Map<String, Object> configMissingBoth = new HashMap<>();
        configMissingBoth.put("sts_token", "my-sts-token");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> MaxcomputeUtil.getAccount(ReadonlyConfig.fromMap(configMissingBoth)),
                "Expected IllegalArgumentException when sts_token is set but accessId/accesskey are missing");
    }

    /** Providing sts_token with only accessId (no accessKey) must also fail fast. */
    @Test
    void testGetAccountThrowsWhenStsTokenProvidedWithoutAccessKey() {
        Map<String, Object> configMissingKey = new HashMap<>();
        configMissingKey.put("accessId", "my-id");
        configMissingKey.put("sts_token", "my-sts-token");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> MaxcomputeUtil.getAccount(ReadonlyConfig.fromMap(configMissingKey)));
    }

    /** Providing sts_token with only accessKey (no accessId) must also fail fast. */
    @Test
    void testGetAccountThrowsWhenStsTokenProvidedWithoutAccessId() {
        Map<String, Object> configMissingId = new HashMap<>();
        configMissingId.put("accesskey", "my-key");
        configMissingId.put("sts_token", "my-sts-token");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> MaxcomputeUtil.getAccount(ReadonlyConfig.fromMap(configMissingId)));
    }

    // --- getOdps() / schema_name propagation test ---

    /**
     * When schema_name is provided, it must be propagated to Odps.getCurrentSchema(). This verifies
     * that non-default MaxCompute schemas are correctly set on the Odps client, enabling
     * schema-aware table routing for source and sink operations.
     */
    @Test
    void testGetOdpsSetsCurrentSchemaWhenSchemaNameProvided() {
        Map<String, Object> config = new HashMap<>();
        config.put("accessId", "my-id");
        config.put("accesskey", "my-key");
        config.put("endpoint", "http://service.odps.aliyun.com/api");
        config.put("project", "my_project");
        config.put("schema_name", "my_schema");

        com.aliyun.odps.Odps odps = MaxcomputeUtil.getOdps(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals("my_schema", odps.getCurrentSchema());
    }

    /**
     * When schema_name is absent, Odps.getCurrentSchema() must remain null, letting the SDK fall
     * back to the project's default schema transparently.
     */
    @Test
    void testGetOdpsLeavesCurrentSchemaEmptyWhenSchemaNameAbsent() {
        Map<String, Object> config = new HashMap<>();
        config.put("accessId", "my-id");
        config.put("accesskey", "my-key");
        config.put("endpoint", "http://service.odps.aliyun.com/api");
        config.put("project", "my_project");

        com.aliyun.odps.Odps odps = MaxcomputeUtil.getOdps(ReadonlyConfig.fromMap(config));

        Assertions.assertNull(odps.getCurrentSchema());
    }
}
