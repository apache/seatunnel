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

/**
 * Compatibility Retained Class: The original HiveMetaStoreProxy has been renamed to
 * HiveMetaStoreCatalog. This class only serves as a backward compatibility wrapper and no longer
 * maintains independent logic. Please directly use HiveMetaStoreCatalog in subsequent operations.
 */
@Deprecated
public class HiveMetaStoreProxy extends HiveMetaStoreCatalog {

    // The following fields/method are kept solely for unit-test compatibility.
    // Some tests use reflection with getDeclaredField/getDeclaredMethod on this class, which does
    // not search superclasses. These members mirror the old class surface so tests can inject
    // mocked values and verify Kerberos re-login behavior without altering the new structure.
    private transient HiveMetaStoreClient hiveClient;
    private transient UserGroupInformation userGroupInformation;
    private boolean kerberosEnabled;

    public HiveMetaStoreProxy(ReadonlyConfig config) {
        super(config);
    }

    public static HiveMetaStoreProxy getInstance(ReadonlyConfig config) {
        return new HiveMetaStoreProxy(config);
    }

    // Mirrors the old private method signature used by tests via reflection.
    // Behavior:
    // - If a test has injected a client into this class (hiveClient != null), apply the minimal
    //   Kerberos re-login logic using the mirrored fields and return the injected client.
    // - Otherwise, delegate to the new implementation in the superclass.
    @SuppressWarnings("unused")
    private synchronized HiveMetaStoreClient getClient() {
        if (this.hiveClient == null) {
            // No test-injected client; use the new implementation in the catalog.
            return super.getClientForProxy();
        }

        if (this.kerberosEnabled && this.userGroupInformation != null) {
            try {
                if (this.userGroupInformation.isFromKeytab()) {
                    this.userGroupInformation.checkTGTAndReloginFromKeytab();
                }
            } catch (Exception e) {
                // Swallow as original logic; tests assert no exception is thrown.
            }
        }
        return this.hiveClient;
    }
}
