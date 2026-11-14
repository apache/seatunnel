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

package org.apache.seatunnel.connectors.seatunnel.hudi.util;

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;

/** Runtime-only identity and resources owned by one catalog or write client. */
public final class HudiAuthenticationContext {

    private final Configuration configuration;
    private final UserGroupInformation user;
    private ForkJoinPool pool;
    private volatile boolean closed;

    public HudiAuthenticationContext(
            Configuration configuration, String principal, String keytab, String krb5Path) {
        this.configuration = configuration;
        boolean hasPrincipal = principal != null && !principal.trim().isEmpty();
        boolean hasKeytab = keytab != null && !keytab.trim().isEmpty();
        if ((principal != null || keytab != null) && (!hasPrincipal || !hasKeytab)) {
            throw failure("Both kerberos_principal and kerberos_keytab_path must be nonblank");
        }
        if (!hasPrincipal) {
            user = null;
            return;
        }
        if (!"kerberos".equalsIgnoreCase(configuration.get("hadoop.security.authentication"))) {
            throw failure(
                    "Kerberos options require hadoop.security.authentication=kerberos in conf_files_path");
        }
        try {
            synchronized (UserGroupInformation.class) {
                if (krb5Path != null && !krb5Path.trim().isEmpty()) {
                    System.setProperty("java.security.krb5.conf", krb5Path);
                }
                UserGroupInformation.setConfiguration(configuration);
                user = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
            }
        } catch (IOException | RuntimeException e) {
            // Hadoop login exceptions can contain the principal and keytab path.
            throw failure(
                    "Hudi Kerberos login failed; check credential availability and Hadoop configuration");
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    /** Renew before I/O; nested operations and parallel streams retain this client's identity. */
    public <T> T execute(PrivilegedExceptionAction<T> action) {
        return execute(action, true);
    }

    private <T> T execute(PrivilegedExceptionAction<T> action, boolean renew) {
        if (closed) {
            throw new IllegalStateException("Hudi authentication context is closed");
        }
        if (user == null) {
            try {
                return action.run();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new HudiConnectorException(
                        CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                        "Hudi operation failed",
                        e);
            }
        }
        if (renew) {
            try {
                synchronized (user) {
                    user.checkTGTAndReloginFromKeytab();
                }
            } catch (IOException | RuntimeException e) {
                throw failure("Hudi Kerberos ticket renewal failed");
            }
        }
        ForkJoinPool executor = getPool();
        try {
            if (ForkJoinTask.getPool() == executor) {
                return action.run();
            }
            return executor.submit(() -> user.doAs(action)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw failure("Hudi authenticated operation interrupted");
        } catch (ExecutionException | RuntimeException e) {
            throw failure("Hudi authenticated operation failed");
        } catch (Exception e) {
            throw failure("Hudi authenticated operation failed");
        }
    }

    private synchronized ForkJoinPool getPool() {
        if (pool == null) {
            // Hudi's Java engine and HoodieListData use parallel streams. The common pool
            // may already exist outside doAs, so scope the whole private worker lifetime.
            pool =
                    new ForkJoinPool(
                            Math.min(
                                    32767, Math.max(1, Runtime.getRuntime().availableProcessors())),
                            p ->
                                    new ForkJoinWorkerThread(p) {
                                        @Override
                                        public void run() {
                                            try {
                                                user.doAs(
                                                        (PrivilegedExceptionAction<Void>)
                                                                () -> {
                                                                    super.run();
                                                                    return null;
                                                                });
                                            } catch (InterruptedException e) {
                                                Thread.currentThread().interrupt();
                                            } catch (IOException e) {
                                                throw failure("Hudi authenticated worker failed");
                                            }
                                        }
                                    },
                            null,
                            false);
        }
        return pool;
    }

    /** Cleanup still runs under the identity if ticket renewal has failed. */
    public void close(PrivilegedExceptionAction<Void> cleanup) {
        if (closed) {
            return;
        }
        RuntimeException failure = null;
        try {
            execute(cleanup, false);
        } catch (RuntimeException e) {
            failure = e;
        } finally {
            closed = true;
            if (pool != null) {
                pool.shutdown();
                try {
                    if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                        pool.shutdownNow();
                        failure = failure("Hudi authenticated worker shutdown timed out");
                    }
                } catch (InterruptedException e) {
                    pool.shutdownNow();
                    Thread.currentThread().interrupt();
                    failure = failure("Hudi authenticated cleanup interrupted");
                }
            }
            if (user != null) {
                try {
                    user.doAs(
                            (PrivilegedExceptionAction<Void>)
                                    () -> {
                                        FileSystem.closeAllForUGI(user);
                                        return null;
                                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failure = failure("Hudi authenticated filesystem cleanup interrupted");
                } catch (IOException | RuntimeException e) {
                    failure = failure("Hudi authenticated filesystem cleanup failed");
                } finally {
                    try {
                        user.logoutUserFromKeytab();
                    } catch (IOException | RuntimeException e) {
                        failure = failure("Hudi Kerberos logout failed");
                    }
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static HudiConnectorException failure(String message) {
        return new HudiConnectorException(
                CommonErrorCodeDeprecated.KERBEROS_AUTHORIZED_FAILED, message);
    }
}
