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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import io.debezium.connector.postgresql.connection.Lsn;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

/**
 * Checkpoint-aware GaussDB logical replication stream for {@code mppdb_decoding}.
 *
 * <p>The Huawei and PostgreSQL JDBC replication APIs are invoked through their common method
 * contract so deployments can supply either compatible driver. SQL peek functions are used only
 * when the installed driver or database port does not expose the replication protocol.
 */
@Slf4j
final class MppdbReplicationStream implements AutoCloseable {

    /** SQLSTATE raised when a concurrent reader has already created the same slot. */
    private static final String DUPLICATE_OBJECT_SQL_STATE = "42710";

    /** Regular JDBC connection used for slot management and SQL polling fallback. */
    private final Connection dataConnection;

    /** Validated mppdb runtime settings. */
    private final GaussDBMppdbConfig config;

    /** Database username used for the dedicated replication connection. */
    private final String username;

    /** Database password used for the dedicated replication connection. */
    private final String password;

    /** JDBC fetch size applied to SQL fallback reads. */
    private final int fetchSize;

    /** Strict binary protocol decoder. */
    private final MppdbBinaryDecoder binaryDecoder = new MppdbBinaryDecoder();

    /** Structured JSON and text protocol decoder. */
    private final MppdbTextDecoder textDecoder = new MppdbTextDecoder();

    /** SQL rows read but not yet acknowledged by a completed SeaTunnel checkpoint. */
    private final List<Long> unacknowledgedSqlLsns = new ArrayList<>();

    /** Dedicated replication-protocol JDBC connection, when supported. */
    private Connection replicationConnection;

    /** Driver-specific PGReplicationStream object accessed through reflection. */
    private Object replicationStream;

    /** Whether records are currently read from the replication API instead of SQL polling. */
    private boolean replicationApiActive;

    /** Whether this stream accepts further reads. */
    private volatile boolean running;

    /** Creates a stream bound to one source-reader task context. */
    MppdbReplicationStream(
            Connection dataConnection,
            GaussDBMppdbConfig config,
            String username,
            String password,
            int fetchSize) {
        this.dataConnection = dataConnection;
        this.config = config;
        this.username = username;
        this.password = password;
        this.fetchSize = fetchSize;
    }

    /** Ensures the configured logical slot exists and uses {@code mppdb_decoding}. */
    synchronized void ensureSlot() throws SQLException {
        String existingPlugin = findSlotPlugin();
        if (existingPlugin != null) {
            verifySlotPlugin(existingPlugin);
            return;
        }
        try (PreparedStatement statement =
                dataConnection.prepareStatement(
                        "SELECT * FROM pg_create_logical_replication_slot(?, ?)")) {
            statement.setString(1, config.getSlotName());
            statement.setString(2, config.getPluginName());
            statement.execute();
        } catch (SQLException e) {
            if (!DUPLICATE_OBJECT_SQL_STATE.equals(e.getSQLState())) {
                throw new SQLException(
                        "Failed to create GaussDB logical replication slot '"
                                + config.getSlotName()
                                + "' with plugin '"
                                + config.getPluginName()
                                + "'",
                        e);
            }
            String concurrentPlugin = findSlotPlugin();
            if (concurrentPlugin == null) {
                throw new SQLException(
                        "GaussDB reported that logical replication slot '"
                                + config.getSlotName()
                                + "' already exists, but the slot could not be queried",
                        e);
            }
            verifySlotPlugin(concurrentPlugin);
        }
        log.info(
                "Prepared GaussDB logical replication slot '{}' with plugin '{}'",
                config.getSlotName(),
                config.getPluginName());
    }

    /** Starts reading from the supplied checkpoint or snapshot boundary. */
    synchronized void start(long startLsn) throws SQLException {
        ensureSlot();
        try {
            initializeReplicationApi(startLsn);
            replicationApiActive = true;
            log.info(
                    "Started GaussDB mppdb_decoding replication stream for slot '{}' at {}",
                    config.getSlotName(),
                    Lsn.valueOf(startLsn).asString());
        } catch (Exception e) {
            closeReplicationApi();
            replicationApiActive = false;
            if (startLsn != 0) {
                advanceSlot(startLsn);
            }
            log.warn(
                    "GaussDB JDBC replication API is unavailable for slot '{}'; using checkpoint-aware SQL polling: {}",
                    config.getSlotName(),
                    rootMessage(e));
        }
        running = true;
    }

    /** Reads up to {@code maxChanges} currently available records without blocking indefinitely. */
    synchronized List<MppdbWalChange> readPending(int maxChanges) throws SQLException {
        if (!running) {
            return new ArrayList<>();
        }
        return replicationApiActive
                ? readFromReplicationApi(maxChanges)
                : readFromSqlFunction(maxChanges);
    }

    /** Advances the server flush position only after SeaTunnel completes a checkpoint. */
    synchronized void acknowledge(long checkpointLsn) throws SQLException {
        if (checkpointLsn == 0) {
            return;
        }
        if (replicationApiActive && replicationStream != null) {
            Object driverLsn = createDriverLsn(parameterType("setFlushedLSN"), checkpointLsn);
            invoke(replicationStream, "setFlushedLSN", driverLsn);
            Method appliedMethod = findMethod(replicationStream.getClass(), "setAppliedLSN", 1);
            if (appliedMethod != null) {
                invoke(replicationStream, appliedMethod, driverLsn);
            }
            invoke(replicationStream, "forceUpdateStatus");
        } else {
            advanceSlot(checkpointLsn);
            Iterator<Long> iterator = unacknowledgedSqlLsns.iterator();
            while (iterator.hasNext()) {
                long rowLsn = iterator.next();
                if (Lsn.valueOf(rowLsn).compareTo(Lsn.valueOf(checkpointLsn)) <= 0) {
                    iterator.remove();
                }
            }
        }
    }

    /** Returns whether the reader loop should continue polling. */
    boolean isRunning() {
        return running;
    }

    /** Closes only client connections; the persistent logical slot is intentionally retained. */
    @Override
    public synchronized void close() {
        running = false;
        closeReplicationApi();
    }

    /** Reads records from the JDBC logical replication protocol. */
    private List<MppdbWalChange> readFromReplicationApi(int maxChanges) throws SQLException {
        List<MppdbWalChange> changes = new ArrayList<>();
        if ((Boolean) invoke(replicationStream, "isClosed")) {
            throw new SQLException(
                    "GaussDB logical replication stream for slot '"
                            + config.getSlotName()
                            + "' closed unexpectedly");
        }
        while (changes.size() < maxChanges) {
            ByteBuffer payload = (ByteBuffer) invoke(replicationStream, "readPending");
            if (payload == null || !payload.hasRemaining()) {
                break;
            }
            byte[] bytes = new byte[payload.remaining()];
            payload.get(bytes);
            long receiveLsn = readReceiveLsn();
            List<MppdbWalChange> decoded = decodeReplicationPayload(bytes);
            for (MppdbWalChange change : decoded) {
                changes.add(change.getLsn() == 0 ? copyWithLsn(change, receiveLsn) : change);
            }
            // A status message keeps the replication connection alive. Flushed/applied LSNs are
            // deliberately unchanged until acknowledge() receives a completed checkpoint.
            invoke(replicationStream, "forceUpdateStatus");
            if (decoded.isEmpty()) {
                break;
            }
        }
        return changes;
    }

    /** Reads rows through pg_logical_slot_peek_changes without consuming uncheckpointed WAL. */
    private List<MppdbWalChange> readFromSqlFunction(int maxChanges) throws SQLException {
        int alreadyRead = unacknowledgedSqlLsns.size();
        int queryLimit =
                (int) Math.min(Integer.MAX_VALUE, (long) alreadyRead + Math.max(1, maxChanges));
        StringBuilder options = new StringBuilder("'include-xids', '1'");
        if (config.getParallelDecodeNum() > 1) {
            options.append(", 'parallel-decode-num', '")
                    .append(config.getParallelDecodeNum())
                    .append("'");
        }
        String sql =
                "SELECT location AS lsn, xid, data FROM pg_logical_slot_peek_changes(?, NULL, ?, "
                        + options
                        + ")";
        List<MppdbWalChange> changes = new ArrayList<>();
        try (PreparedStatement statement = dataConnection.prepareStatement(sql)) {
            statement.setFetchSize(fetchSize);
            statement.setString(1, config.getSlotName());
            statement.setInt(2, queryLimit);
            try (ResultSet resultSet = statement.executeQuery()) {
                int rowIndex = 0;
                while (resultSet.next()) {
                    rowIndex++;
                    if (rowIndex <= alreadyRead) {
                        continue;
                    }
                    long lsn = Lsn.valueOf(resultSet.getString("lsn")).asLong();
                    long transactionId = resultSet.getLong("xid");
                    String data = resultSet.getString("data");
                    unacknowledgedSqlLsns.add(lsn);
                    changes.add(textDecoder.decodeRecord(lsn, transactionId, data));
                    if (changes.size() >= maxChanges) {
                        break;
                    }
                }
            }
        }
        return changes;
    }

    /** Selects the binary or textual decoder according to effective server slot options. */
    private List<MppdbWalChange> decodeReplicationPayload(byte[] payload) {
        if (config.getParallelDecodeNum() > 1 && "b".equals(config.getDecodeStyle())) {
            return binaryDecoder.decode(payload);
        }
        if (config.getParallelDecodeNum() > 1) {
            return textDecoder.decodeBatch(payload);
        }
        return textDecoder.decodeBatch(payload);
    }

    /** Creates a driver-specific replication stream through the common PG JDBC fluent API. */
    private void initializeReplicationApi(long startLsn) throws Exception {
        String replicationUrl = buildReplicationUrl();
        replicationConnection = DriverManager.getConnection(replicationUrl, username, password);
        Object pgConnection = unwrapPgConnection(replicationConnection);
        Object replicationApi = invoke(pgConnection, "getReplicationAPI");
        Object streamBuilder = invoke(replicationApi, "replicationStream");
        Object logicalBuilder = invoke(streamBuilder, "logical");
        Object slotBuilder = invoke(logicalBuilder, "withSlotName", config.getSlotName());

        if (startLsn != 0) {
            Method startPosition = findMethod(slotBuilder.getClass(), "withStartPosition", 1);
            if (startPosition == null) {
                throw new NoSuchMethodException("withStartPosition");
            }
            slotBuilder =
                    invoke(
                            slotBuilder,
                            startPosition,
                            createDriverLsn(startPosition.getParameterTypes()[0], startLsn));
        }
        if (config.getParallelDecodeNum() > 1) {
            slotBuilder =
                    invoke(
                            slotBuilder,
                            "withSlotOption",
                            "parallel-decode-num",
                            String.valueOf(config.getParallelDecodeNum()));
            slotBuilder =
                    invoke(slotBuilder, "withSlotOption", "decode-style", config.getDecodeStyle());
            if (config.isSendingBatch()) {
                slotBuilder = invoke(slotBuilder, "withSlotOption", "sending-batch", "1");
            }
        }
        slotBuilder = invoke(slotBuilder, "withSlotOption", "include-xids", "1");
        slotBuilder = invoke(slotBuilder, "withSlotOption", "include-timestamp", "1");
        replicationStream = invoke(slotBuilder, "start");
    }

    /** Unwraps either the Huawei or PostgreSQL PGConnection interface without a hard dependency. */
    private Object unwrapPgConnection(Connection connection) throws Exception {
        String[] candidates = {
            "com.huawei.gaussdb.jdbc.PGConnection", "org.postgresql.PGConnection"
        };
        ClassLoader classLoader = connection.getClass().getClassLoader();
        for (String candidate : candidates) {
            try {
                Class<?> type = Class.forName(candidate, false, classLoader);
                if (type.isInstance(connection)) {
                    return connection;
                }
                if (connection.isWrapperFor(type)) {
                    return connection.unwrap(type);
                }
            } catch (ClassNotFoundException ignored) {
                // The deployment only needs one compatible driver family.
            }
        }
        if (findMethod(connection.getClass(), "getReplicationAPI", 0) != null) {
            return connection;
        }
        throw new SQLException(
                "The configured JDBC driver does not expose a PostgreSQL-compatible replication API");
    }

    /** Adds replication protocol parameters and optionally rewrites the dedicated server port. */
    private String buildReplicationUrl() {
        String url = config.getJdbcUrl();
        if (config.getReplicationPort() != null) {
            url =
                    url.replaceFirst(
                            "(jdbc:[^:]+://(?:\\[[^]]+]|[^:/?#]+)):\\d+(/)",
                            "$1:" + config.getReplicationPort() + "$2");
        }
        String separator = url.contains("?") ? "&" : "?";
        StringBuilder result = new StringBuilder(url);
        if (!containsQueryParameter(url, "replication")) {
            result.append(separator).append("replication=database");
            separator = "&";
        }
        if (!containsQueryParameter(url, "preferQueryMode")) {
            result.append(separator).append("preferQueryMode=simple");
            separator = "&";
        }
        if (!containsQueryParameter(url, "assumeMinServerVersion")) {
            result.append(separator).append("assumeMinServerVersion=9.4");
        }
        return result.toString();
    }

    /** Returns whether a JDBC query string already contains the supplied parameter. */
    private boolean containsQueryParameter(String url, String parameter) {
        String lower = url.toLowerCase(Locale.ROOT);
        String key = parameter.toLowerCase(Locale.ROOT) + "=";
        int queryStart = lower.indexOf('?');
        return queryStart >= 0
                && (lower.substring(queryStart + 1).startsWith(key)
                        || lower.substring(queryStart + 1).contains("&" + key));
    }

    /**
     * Returns the plugin recorded for the configured slot, or null when the slot does not exist.
     */
    private String findSlotPlugin() throws SQLException {
        try (PreparedStatement statement =
                dataConnection.prepareStatement(
                        "SELECT plugin FROM pg_replication_slots WHERE slot_name = ?")) {
            statement.setString(1, config.getSlotName());
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getString(1) : null;
            }
        }
    }

    /** Rejects accidental reuse of a slot created for a different decoding plugin. */
    private void verifySlotPlugin(String actualPlugin) throws SQLException {
        if (!config.getPluginName().equalsIgnoreCase(actualPlugin)) {
            throw new SQLException(
                    "GaussDB logical replication slot '"
                            + config.getSlotName()
                            + "' uses plugin '"
                            + actualPlugin
                            + "', expected '"
                            + config.getPluginName()
                            + "'");
        }
    }

    /**
     * Advances a logical slot using the GaussDB function with an openGauss compatibility fallback.
     */
    private void advanceSlot(long checkpointLsn) throws SQLException {
        String target = Lsn.valueOf(checkpointLsn).asString();
        String[] statements = {
            "SELECT pg_replication_slot_advance(?, ?)", "SELECT pg_logical_slot_advance(?, ?)"
        };
        SQLException failure = null;
        for (String sql : statements) {
            try (PreparedStatement statement = dataConnection.prepareStatement(sql)) {
                statement.setString(1, config.getSlotName());
                statement.setString(2, target);
                statement.execute();
                return;
            } catch (SQLException e) {
                failure = e;
            }
        }
        throw new SQLException(
                "Failed to acknowledge GaussDB logical replication slot '"
                        + config.getSlotName()
                        + "' at "
                        + target,
                failure);
    }

    /** Reads the driver's last receive position as Debezium's long LSN representation. */
    private long readReceiveLsn() throws SQLException {
        Object driverLsn = invoke(replicationStream, "getLastReceiveLSN");
        if (driverLsn == null) {
            return 0;
        }
        Object value = invoke(driverLsn, "asLong");
        return ((Number) value).longValue();
    }

    /** Creates the LSN value class expected by the active JDBC driver. */
    private Object createDriverLsn(Class<?> type, long lsn) throws SQLException {
        Method valueOf = findStaticMethod(type, "valueOf", String.class);
        if (valueOf != null) {
            return invoke(null, valueOf, Lsn.valueOf(lsn).asString());
        }
        valueOf = findStaticMethod(type, "valueOf", long.class);
        if (valueOf != null) {
            return invoke(null, valueOf, lsn);
        }
        throw new SQLException("JDBC replication LSN type does not expose valueOf: " + type);
    }

    /** Returns the single parameter type for a driver stream method. */
    private Class<?> parameterType(String methodName) throws SQLException {
        Method method = findMethod(replicationStream.getClass(), methodName, 1);
        if (method == null) {
            throw new SQLException("JDBC replication stream does not expose " + methodName);
        }
        return method.getParameterTypes()[0];
    }

    /** Copies a serial record while applying its replication-stream LSN. */
    private MppdbWalChange copyWithLsn(MppdbWalChange change, long lsn) {
        return new MppdbWalChange(
                lsn,
                change.getTransactionId(),
                change.getType(),
                change.getSchema(),
                change.getTable(),
                change.getOldColumns(),
                change.getNewColumns());
    }

    /** Invokes a named driver method with consistent SQLException unwrapping. */
    private Object invoke(Object target, String methodName, Object... arguments)
            throws SQLException {
        Method method = findCompatibleMethod(target.getClass(), methodName, arguments);
        if (method == null) {
            throw new SQLException(
                    "JDBC replication type "
                            + target.getClass().getName()
                            + " does not expose "
                            + methodName);
        }
        return invoke(target, method, arguments);
    }

    /** Invokes an already-resolved driver method and exposes its root failure. */
    private Object invoke(Object target, Method method, Object... arguments) throws SQLException {
        try {
            return method.invoke(target, arguments);
        } catch (IllegalAccessException e) {
            throw new SQLException("Cannot access JDBC replication method " + method.getName(), e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SQLException) {
                throw (SQLException) cause;
            }
            throw new SQLException("JDBC replication method failed: " + method.getName(), cause);
        }
    }

    /** Finds a public method on a driver class or one of its public interfaces. */
    private Method findMethod(Class<?> type, String name, int parameterCount) {
        for (Method method : type.getMethods()) {
            if (method.getName().equals(name) && method.getParameterCount() == parameterCount) {
                return method;
            }
        }
        return null;
    }

    /** Finds a public overload whose parameter types accept the supplied reflection arguments. */
    private Method findCompatibleMethod(Class<?> type, String name, Object[] arguments) {
        for (Method method : type.getMethods()) {
            if (!method.getName().equals(name) || method.getParameterCount() != arguments.length) {
                continue;
            }
            Class<?>[] parameterTypes = method.getParameterTypes();
            boolean compatible = true;
            for (int index = 0; index < arguments.length; index++) {
                if (arguments[index] != null
                        && !wrap(parameterTypes[index]).isInstance(arguments[index])) {
                    compatible = false;
                    break;
                }
            }
            if (compatible) {
                return method;
            }
        }
        return null;
    }

    /** Converts primitive parameter types to wrappers for reflection compatibility checks. */
    private Class<?> wrap(Class<?> type) {
        if (!type.isPrimitive()) {
            return type;
        }
        if (type == boolean.class) {
            return Boolean.class;
        }
        if (type == int.class) {
            return Integer.class;
        }
        if (type == long.class) {
            return Long.class;
        }
        if (type == byte.class) {
            return Byte.class;
        }
        if (type == short.class) {
            return Short.class;
        }
        if (type == float.class) {
            return Float.class;
        }
        if (type == double.class) {
            return Double.class;
        }
        if (type == char.class) {
            return Character.class;
        }
        return type;
    }

    /** Finds a static value factory with the exact argument expected by a driver LSN class. */
    private Method findStaticMethod(Class<?> type, String name, Class<?> parameterType) {
        try {
            return type.getMethod(name, parameterType);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /** Closes replication API resources after failure or task shutdown. */
    private void closeReplicationApi() {
        if (replicationStream != null) {
            try {
                invoke(replicationStream, "close");
            } catch (SQLException e) {
                log.debug("Failed to close GaussDB replication stream", e);
            } finally {
                replicationStream = null;
            }
        }
        if (replicationConnection != null) {
            try {
                replicationConnection.close();
            } catch (SQLException e) {
                log.debug("Failed to close GaussDB replication connection", e);
            } finally {
                replicationConnection = null;
            }
        }
    }

    /** Returns a concise nested exception message without logging credentials or JDBC URLs. */
    private String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null
                ? current.getClass().getSimpleName()
                : current.getMessage();
    }
}
