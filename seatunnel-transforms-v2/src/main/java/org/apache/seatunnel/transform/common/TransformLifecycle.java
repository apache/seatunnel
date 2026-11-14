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

package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/** Runtime ownership of direct children; kept transient by each serializable wrapper. */
final class TransformLifecycle {
    private final List<SeaTunnelTransform<SeaTunnelRow>> children = new ArrayList<>();
    private boolean opened;
    private boolean opening;
    private boolean closed;
    private Exception startupFailure;
    private boolean closeCompleted;
    private Thread closeOwner;
    private Exception closeFailure;

    TransformLifecycle(Collection<SeaTunnelTransform<SeaTunnelRow>> children) {
        Set<SeaTunnelTransform<SeaTunnelRow>> seen =
                Collections.newSetFromMap(new IdentityHashMap<>());
        for (SeaTunnelTransform<SeaTunnelRow> child : children) {
            if (seen.add(child)) {
                this.children.add(child);
            }
        }
    }

    /**
     * Initialize once, retaining the original failure and releasing partially initialized owners.
     */
    @SneakyThrows
    void open() {
        synchronized (this) {
            while (opening) {
                wait();
            }
            if (startupFailure != null) {
                throw startupFailure;
            }
            if (closed) {
                throw new IllegalStateException("Transform lifecycle is closed");
            }
            if (opened) {
                return;
            }
            opening = true;
        }
        try {
            for (SeaTunnelTransform<SeaTunnelRow> child : children) {
                synchronized (this) {
                    if (closed) {
                        throw new IllegalStateException(
                                "Transform lifecycle was closed during open");
                    }
                }
                child.open();
            }
            synchronized (this) {
                if (closed) {
                    throw new IllegalStateException("Transform lifecycle was closed during open");
                }
                opened = true;
            }
        } catch (Exception failure) {
            synchronized (this) {
                startupFailure = failure;
            }
            try {
                close();
            } catch (Exception cleanupFailure) {
                suppress(failure, cleanupFailure);
            }
            throw failure;
        } finally {
            synchronized (this) {
                opening = false;
                notifyAll();
            }
        }
    }

    /** Attempt every owned close once, including children initialized during schema discovery. */
    @SneakyThrows
    void close() {
        boolean interrupted = false;
        synchronized (this) {
            if (closed) {
                while (!closeCompleted && closeOwner != Thread.currentThread()) {
                    try {
                        wait();
                    } catch (InterruptedException ignored) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                if (closeFailure != null) {
                    throw closeFailure;
                }
                return;
            }
            closed = true;
            closeOwner = Thread.currentThread();
        }
        // Do not hold the owner monitor while calling plugins. In particular, Python close must
        // be able to stop a child whose open hook is blocked waiting for the subprocess.
        Exception failure = null;
        try {
            for (SeaTunnelTransform<SeaTunnelRow> child : children) {
                try {
                    child.close();
                } catch (Exception cleanupFailure) {
                    if (failure == null) {
                        failure = cleanupFailure;
                    } else {
                        suppress(failure, cleanupFailure);
                    }
                }
            }
        } finally {
            synchronized (this) {
                closeFailure = failure;
                closeCompleted = true;
                closeOwner = null;
                notifyAll();
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** Release children when construction/schema discovery fails before publishing their owner. */
    static void closeAfterFailure(
            Collection<SeaTunnelTransform<SeaTunnelRow>> children, Exception failure) {
        try {
            new TransformLifecycle(children).close();
        } catch (Exception cleanupFailure) {
            suppress(failure, cleanupFailure);
        }
    }

    private static void suppress(Exception failure, Exception cleanupFailure) {
        if (failure == cleanupFailure) {
            return;
        }
        for (Throwable suppressed : failure.getSuppressed()) {
            if (suppressed == cleanupFailure) {
                return;
            }
        }
        failure.addSuppressed(cleanupFailure);
    }
}
