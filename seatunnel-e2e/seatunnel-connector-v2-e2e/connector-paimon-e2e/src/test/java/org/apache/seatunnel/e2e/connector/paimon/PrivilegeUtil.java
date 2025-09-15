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

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.privilege.PrivilegeChecker;
import org.apache.paimon.privilege.PrivilegeType;
import org.apache.paimon.privilege.PrivilegedCatalog;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public class PrivilegeUtil {
    private PrivilegeUtil() {}

    public static void awaitPrivilegeApplied(
            PrivilegedCatalog privilegedCatalog,
            List<PrivilegeType> privilegeTypes,
            List<Identifier> identifiers) {
        PrivilegeChecker privilegeChecker =
                privilegedCatalog.privilegeManager().getPrivilegeChecker();
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertDoesNotThrow(
                                    () -> {
                                        for (PrivilegeType type : privilegeTypes) {
                                            switch (type) {
                                                case SELECT:
                                                    for (Identifier tableIdentifier : identifiers) {
                                                        privilegeChecker.assertCanSelect(
                                                                tableIdentifier);
                                                    }
                                                    break;
                                                case INSERT:
                                                    for (Identifier tableIdentifier : identifiers) {
                                                        privilegeChecker.assertCanInsert(
                                                                tableIdentifier);
                                                    }
                                                    break;
                                                case ALTER_TABLE:
                                                    for (Identifier tableIdentifier : identifiers) {
                                                        privilegeChecker.assertCanAlterTable(
                                                                tableIdentifier);
                                                    }
                                                    break;
                                                case DROP_TABLE:
                                                    for (Identifier tableIdentifier : identifiers) {
                                                        privilegeChecker.assertCanDropTable(
                                                                tableIdentifier);
                                                    }
                                                    break;
                                                case CREATE_TABLE:
                                                    for (Identifier tableIdentifier : identifiers) {
                                                        privilegeChecker.assertCanCreateTable(
                                                                tableIdentifier.getDatabaseName());
                                                    }
                                                    break;
                                                case DROP_DATABASE:
                                                    for (Identifier tableIdentifier : identifiers) {
                                                        privilegeChecker.assertCanDropDatabase(
                                                                tableIdentifier.getDatabaseName());
                                                    }
                                                    break;
                                                case ALTER_DATABASE:
                                                    for (Identifier tableIdentifier : identifiers) {
                                                        privilegeChecker.assertCanAlterDatabase(
                                                                tableIdentifier.getDatabaseName());
                                                    }
                                                    break;
                                                case CREATE_DATABASE:
                                                    privilegeChecker.assertCanCreateDatabase();
                                                    break;
                                                case ADMIN:
                                                    for (Identifier tableIdentifier : identifiers) {
                                                        privilegeChecker.assertCanSelect(
                                                                tableIdentifier);
                                                        privilegeChecker.assertCanInsert(
                                                                tableIdentifier);
                                                        privilegeChecker.assertCanAlterTable(
                                                                tableIdentifier);
                                                        privilegeChecker.assertCanDropTable(
                                                                tableIdentifier);
                                                        privilegeChecker.assertCanCreateTable(
                                                                tableIdentifier.getDatabaseName());
                                                        privilegeChecker.assertCanDropDatabase(
                                                                tableIdentifier.getDatabaseName());
                                                        privilegeChecker.assertCanAlterDatabase(
                                                                tableIdentifier.getDatabaseName());
                                                        privilegeChecker.assertCanCreateDatabase();
                                                    }
                                                    break;
                                                default:
                                                    throw new UnsupportedOperationException(
                                                            "Unsupported privilege type: " + type);
                                            }
                                        }
                                    });
                        });
    }
}
