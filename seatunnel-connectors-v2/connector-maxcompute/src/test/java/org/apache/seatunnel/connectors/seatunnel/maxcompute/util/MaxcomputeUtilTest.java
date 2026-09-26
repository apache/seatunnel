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
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeBaseOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AklessAccount;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.commons.GeneralConfiguration;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.TableTunnel;

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

        Odps odps = MaxcomputeUtil.getOdps(ReadonlyConfig.fromMap(config));

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

        Odps odps = MaxcomputeUtil.getOdps(ReadonlyConfig.fromMap(config));

        Assertions.assertNull(odps.getCurrentSchema());
    }

    // --- getOdps() / getTableTunnel() timeout & retry wiring tests ---
    //
    // The ODPS SDK exposes two independent HTTP clients:
    //   * REST client   (Odps.getRestClient())     -> control plane: metadata / catalog calls
    //   * Tunnel client (TableTunnel.getConfig())  -> data plane: bulk row read / write / upsert
    // Each is configured from its own set of options, so the tests below cover per-option
    // application, the option defaults that flow through when options are omitted, the
    // milliseconds->seconds validation guard, and that the two clients never cross-contaminate.

    /** Minimal config that lets getOdps()/getTableTunnel() build a client without network calls. */
    private static Map<String, Object> baseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("accessId", "my-id");
        config.put("accesskey", "my-key");
        config.put("endpoint", "http://service.odps.aliyun.com/api");
        config.put("project", "my_project");
        return config;
    }

    /**
     * A user-supplied connect_timeout_ms must be applied to the ODPS REST client (converted to
     * seconds, since the SDK stores connect/read timeout in seconds internally). This verifies the
     * control-plane client timeout override wiring in getOdps().
     */
    @Test
    void testGetOdpsAppliesRestClientConnectTimeout() {
        Map<String, Object> config = baseConfig();
        config.put("connect_timeout_ms", 30000L);

        Odps odps = MaxcomputeUtil.getOdps(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(30, odps.getRestClient().getConnectTimeout());
    }

    /** read_timeout_ms must be applied to the REST client's read timeout (seconds). */
    @Test
    void testGetOdpsAppliesRestClientReadTimeout() {
        Map<String, Object> config = baseConfig();
        config.put("read_timeout_ms", 60000L);

        Odps odps = MaxcomputeUtil.getOdps(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(60, odps.getRestClient().getReadTimeout());
    }

    /** retry_times must be applied to the REST client's retry count. */
    @Test
    void testGetOdpsAppliesRestClientRetryTimes() {
        Map<String, Object> config = baseConfig();
        config.put("retry_times", 7);

        Odps odps = MaxcomputeUtil.getOdps(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(7, odps.getRestClient().getRetryTimes());
    }

    /**
     * When no REST timeout/retry options are supplied, the connector must fall back to the option
     * defaults (connect 10s, read 120s, retry 4) — i.e. the original SDK behavior is preserved.
     * This guards against accidental regressions in backward compatibility.
     */
    @Test
    void testGetOdpsUsesRestClientDefaultsWhenTimeoutOptionsAbsent() {
        Odps odps = MaxcomputeUtil.getOdps(ReadonlyConfig.fromMap(baseConfig()));

        Assertions.assertEquals(10, odps.getRestClient().getConnectTimeout());
        Assertions.assertEquals(120, odps.getRestClient().getReadTimeout());
        Assertions.assertEquals(4, odps.getRestClient().getRetryTimes());
    }

    /** tunnel_connect_timeout_ms must be applied to the Tunnel client's socket connect timeout. */
    @Test
    void testGetTableTunnelAppliesSocketConnectTimeout() {
        Map<String, Object> config = baseConfig();
        config.put("tunnel_connect_timeout_ms", 240000L);

        TableTunnel tableTunnel = MaxcomputeUtil.getTableTunnel(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(240, tableTunnel.getConfig().getSocketConnectTimeout());
    }

    /**
     * A user-supplied tunnel_read_timeout_ms must be applied to the Tunnel client's socket read
     * timeout (converted to seconds), independent of the ODPS REST client. This verifies the
     * data-plane client timeout override wiring in getTableTunnel().
     */
    @Test
    void testGetTableTunnelAppliesSocketReadTimeout() {
        Map<String, Object> config = baseConfig();
        config.put("tunnel_read_timeout_ms", 600000L);

        TableTunnel tableTunnel = MaxcomputeUtil.getTableTunnel(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(600, tableTunnel.getConfig().getSocketTimeout());
    }

    /** tunnel_retry_times must be applied to the Tunnel client's socket retry count. */
    @Test
    void testGetTableTunnelAppliesSocketRetryTimes() {
        Map<String, Object> config = baseConfig();
        config.put("tunnel_retry_times", 8);

        TableTunnel tableTunnel = MaxcomputeUtil.getTableTunnel(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(8, tableTunnel.getConfig().getSocketRetryTimes());
    }

    /**
     * When no Tunnel timeout/retry options are supplied, the connector must fall back to the option
     * defaults (connect 180s, read 300s, retry 4) — i.e. the original SDK behavior is preserved.
     */
    @Test
    void testGetTableTunnelUsesSocketDefaultsWhenTimeoutOptionsAbsent() {
        TableTunnel tableTunnel =
                MaxcomputeUtil.getTableTunnel(ReadonlyConfig.fromMap(baseConfig()));

        Assertions.assertEquals(180, tableTunnel.getConfig().getSocketConnectTimeout());
        Assertions.assertEquals(300, tableTunnel.getConfig().getSocketTimeout());
        Assertions.assertEquals(4, tableTunnel.getConfig().getSocketRetryTimes());
    }

    /**
     * REST client options must not leak onto the Tunnel client and vice versa: the two clients are
     * configured from disjoint option sets. Setting connect_timeout_ms (REST) must not change the
     * Tunnel socket connect timeout, and setting tunnel_read_timeout_ms must not change the REST
     * read timeout.
     */
    @Test
    void testRestClientAndTunnelClientTimeoutsAreIndependent() {
        Map<String, Object> config = baseConfig();
        config.put("connect_timeout_ms", 30000L); // REST-only option
        config.put("tunnel_read_timeout_ms", 600000L); // Tunnel-only option

        TableTunnel tableTunnel = MaxcomputeUtil.getTableTunnel(ReadonlyConfig.fromMap(config));
        Odps odps = tableTunnel.getConfig().getOdps();

        // REST client picks up the REST option but is untouched by the Tunnel option.
        Assertions.assertEquals(30, odps.getRestClient().getConnectTimeout());
        Assertions.assertEquals(120, odps.getRestClient().getReadTimeout());
        // Tunnel client picks up the Tunnel option but is untouched by the REST option.
        Assertions.assertEquals(180, tableTunnel.getConfig().getSocketConnectTimeout());
        Assertions.assertEquals(600, tableTunnel.getConfig().getSocketTimeout());
    }

    // --- validation tests ---
    //
    // Sub-second / negative timeout values and negative retry counts must be rejected at
    // config-application time rather than silently clamped or passed through to the SDK,
    // because a 0 turning into a 1-second timeout is a nasty surprise that is hard to diagnose.

    /**
     * A sub-second timeout value (e.g. 500ms) must be rejected with a clear error, not silently
     * clamped to 1 second, because the SDK stores timeouts in whole seconds and the user almost
     * certainly did not intend a 1-second timeout.
     */
    @Test
    void testSubSecondTimeoutIsRejected() {
        Map<String, Object> config = baseConfig();
        config.put("connect_timeout_ms", 500L);
        config.put("tunnel_connect_timeout_ms", 999L);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> MaxcomputeUtil.getOdps(ReadonlyConfig.fromMap(config)));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> MaxcomputeUtil.getTableTunnel(ReadonlyConfig.fromMap(config)));
    }

    /** A negative retry_times must be rejected, not passed through to the SDK. */
    @Test
    void testNegativeRetryTimesIsRejected() {
        Map<String, Object> config = baseConfig();
        config.put("retry_times", -1);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> MaxcomputeUtil.getOdps(ReadonlyConfig.fromMap(config)));
    }

    /** A negative tunnel_retry_times must be rejected, not passed through to the SDK. */
    @Test
    void testNegativeTunnelRetryTimesIsRejected() {
        Map<String, Object> config = baseConfig();
        config.put("tunnel_retry_times", -1);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> MaxcomputeUtil.getTableTunnel(ReadonlyConfig.fromMap(config)));
    }

    // --- SDK defaults consistency test ---

    /**
     * The option defaults must match the pinned ODPS SDK (0.51.2) defaults so that omitting the
     * options preserves the original SDK behavior. If a future SDK bump changes its defaults, this
     * test fails loudly and the option defaults can be revisited.
     */
    @Test
    void testOptionDefaultsMatchSdkDefaults() {
        // REST client (control plane)
        Assertions.assertEquals(
                RestClient.DEFAULT_CONNECT_TIMEOUT,
                MaxcomputeBaseOptions.CONNECT_TIMEOUT_MS.defaultValue() / 1000);
        Assertions.assertEquals(
                RestClient.DEFAULT_READ_TIMEOUT,
                MaxcomputeBaseOptions.READ_TIMEOUT_MS.defaultValue() / 1000);
        Assertions.assertEquals(
                RestClient.DEFAULT_CONNECT_RETRYTIMES,
                MaxcomputeBaseOptions.RETRY_TIMES.defaultValue());
        // Tunnel client (data plane)
        Assertions.assertEquals(
                GeneralConfiguration.DEFAULT_SOCKET_CONNECT_TIMEOUT,
                MaxcomputeBaseOptions.TUNNEL_CONNECT_TIMEOUT_MS.defaultValue() / 1000);
        Assertions.assertEquals(
                GeneralConfiguration.DEFAULT_SOCKET_TIMEOUT,
                MaxcomputeBaseOptions.TUNNEL_READ_TIMEOUT_MS.defaultValue() / 1000);
        Assertions.assertEquals(
                GeneralConfiguration.DEFAULT_SOCKET_RETRY_TIMES,
                MaxcomputeBaseOptions.TUNNEL_RETRY_TIMES.defaultValue());
    }
}
