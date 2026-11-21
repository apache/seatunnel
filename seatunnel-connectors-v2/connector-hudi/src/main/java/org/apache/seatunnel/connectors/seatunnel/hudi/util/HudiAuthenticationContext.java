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
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Runtime-only identity and resources owned by one catalog or write client. The owner must finish
 * all operations before closing this context; concurrent close/execute is unsupported. Hadoop's
 * security configuration and krb5 realm settings remain JVM-global, although the returned user and
 * worker pool belong exclusively to this instance.
 */
public final class HudiAuthenticationContext {

    private final Configuration configuration;
    private final UserGroupInformation user;
    // Lazily created for authenticated operations; workers retain this instance's user until close.
    private ForkJoinPool pool;
    // Serial close is idempotent. This flag does not coordinate concurrent resource use/close.
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

    /**
     * Renew before I/O; nested operations and parallel streams retain this client's identity.
     * Interrupting the waiting caller preserves its interrupt flag but does not cancel in-flight
     * I/O.
     */
    public <T> T execute(PrivilegedExceptionAction<T> action) {
        return execute(action, HudiAuthenticationContext::operationFailure);
    }

    /**
     * Execute with a domain-specific failure mapper. The mapper must discard sensitive messages,
     * causes and suppressed exceptions from the supplied action failure. Login/renewal errors do
     * not pass through the mapper. Credential-free execution retains its original exception
     * behavior.
     */
    public <T> T execute(
            PrivilegedExceptionAction<T> action,
            Function<Exception, RuntimeException> failureMapper) {
        return execute(action, true, failureMapper);
    }

    private <T> T execute(
            PrivilegedExceptionAction<T> action,
            boolean renew,
            Function<Exception, RuntimeException> failureMapper) {
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
            throw operationFailure(e);
        } catch (Exception e) {
            Throwable cause = e;
            while ((cause instanceof ExecutionException
                            || cause instanceof PrivilegedActionException)
                    && cause.getCause() != null) {
                cause = cause.getCause();
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw failureMapper.apply((Exception) cause);
        }
    }

    /**
     * Class names only: never include arbitrary exception messages or credential-bearing causes.
     */
    public static String failureCategory(Throwable error) {
        StringBuilder category = new StringBuilder();
        for (int depth = 0; error != null && depth < 8; depth++) {
            if (depth > 0) {
                category.append(" -> ");
            }
            category.append(error.getClass().getSimpleName());
            error = error.getCause();
        }
        return category.toString();
    }

    private static HudiConnectorException operationFailure(Exception error) {
        // Nested provider/client calls have already discarded the unsafe cause chain.
        if (error instanceof OperationFailure) {
            return (OperationFailure) error;
        }
        return new OperationFailure(failureCategory(error));
    }

    private static final class OperationFailure extends HudiConnectorException {
        private OperationFailure(String category) {
            super(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "Hudi operation failed (" + category + ")");
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
            execute(cleanup, false, HudiAuthenticationContext::operationFailure);
        } catch (RuntimeException e) {
            failure = e;
        } finally {
            closed = true;
            if (pool != null) {
                pool.shutdown();
                try {
                    if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                        pool.shutdownNow();
                        failure =
                                accumulate(
                                        failure,
                                        failure("Hudi authenticated worker shutdown timed out"));
                    }
                } catch (InterruptedException e) {
                    pool.shutdownNow();
                    Thread.currentThread().interrupt();
                    failure =
                            accumulate(failure, failure("Hudi authenticated cleanup interrupted"));
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
                    failure =
                            accumulate(
                                    failure,
                                    failure("Hudi authenticated filesystem cleanup interrupted"));
                } catch (IOException | RuntimeException e) {
                    failure =
                            accumulate(
                                    failure,
                                    failure(
                                            "Hudi authenticated filesystem cleanup failed ("
                                                    + failureCategory(e)
                                                    + ")"));
                } finally {
                    try {
                        user.logoutUserFromKeytab();
                    } catch (IOException | RuntimeException e) {
                        failure =
                                accumulate(
                                        failure,
                                        failure(
                                                "Hudi Kerberos logout failed ("
                                                        + failureCategory(e)
                                                        + ")"));
                    }
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException accumulate(RuntimeException first, RuntimeException next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }

    private static HudiConnectorException failure(String message) {
        return new HudiConnectorException(
                CommonErrorCodeDeprecated.KERBEROS_AUTHORIZED_FAILED, message);
    }
}
