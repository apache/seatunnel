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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Temporary LOBs bound to a prepared statement. They stay alive until the batch executes, then
 * {@link #free(PreparedStatement)} releases them so Oracle does not keep the temporary LOBs.
 */
public final class StatementBoundLobs {

    private static final Logger LOG = LoggerFactory.getLogger(StatementBoundLobs.class);
    private static final Map<PreparedStatement, List<Clob>> BOUND =
            new IdentityHashMap<PreparedStatement, List<Clob>>();
    private static final Map<PreparedStatement, List<Blob>> BOUND_BLOBS =
            new IdentityHashMap<PreparedStatement, List<Blob>>();

    private StatementBoundLobs() {}

    public static void track(PreparedStatement statement, Clob clob) {
        synchronized (BOUND) {
            List<Clob> clobs = BOUND.get(statement);
            if (clobs == null) {
                clobs = new ArrayList<Clob>();
                BOUND.put(statement, clobs);
            }
            clobs.add(clob);
        }
    }

    public static void track(PreparedStatement statement, Blob blob) {
        synchronized (BOUND_BLOBS) {
            List<Blob> blobs = BOUND_BLOBS.get(statement);
            if (blobs == null) {
                blobs = new ArrayList<Blob>();
                BOUND_BLOBS.put(statement, blobs);
            }
            blobs.add(blob);
        }
    }

    public static void free(PreparedStatement statement) {
        List<Clob> clobs;
        synchronized (BOUND) {
            clobs = BOUND.remove(statement);
        }
        if (clobs != null) {
            for (Clob clob : clobs) {
                freeLob(clob);
            }
        }
        List<Blob> blobs;
        synchronized (BOUND_BLOBS) {
            blobs = BOUND_BLOBS.remove(statement);
        }
        if (blobs != null) {
            for (Blob blob : blobs) {
                freeLob(blob);
            }
        }
    }

    private static void freeLob(Clob clob) {
        try {
            clob.free();
        } catch (SQLException e) {
            LOG.warn("Failed to free a temporary JDBC LOB", e);
        }
    }

    private static void freeLob(Blob blob) {
        try {
            blob.free();
        } catch (SQLException e) {
            LOG.warn("Failed to free a temporary JDBC LOB", e);
        }
    }
}
