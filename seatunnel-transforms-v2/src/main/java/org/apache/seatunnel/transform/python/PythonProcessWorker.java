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

package org.apache.seatunnel.transform.python;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.format.json.JsonToRowConverters;
import org.apache.seatunnel.format.json.RowToJsonConverters;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.exception.ErrorDataTransformException;
import org.apache.seatunnel.transform.exception.TransformException;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.transform.python.PythonTransformErrorCode.INIT_PYTHON_PROCESS_ERROR;
import static org.apache.seatunnel.transform.python.PythonTransformErrorCode.INVALID_PYTHON_RESULT_ERROR;
import static org.apache.seatunnel.transform.python.PythonTransformErrorCode.LOAD_WORKER_SCRIPT_ERROR;
import static org.apache.seatunnel.transform.python.PythonTransformErrorCode.PYTHON_EXECUTION_ERROR;
import static org.apache.seatunnel.transform.python.PythonTransformErrorCode.PYTHON_PROCESS_TERMINATED_ERROR;
import static org.apache.seatunnel.transform.python.PythonTransformErrorCode.START_PYTHON_PROCESS_ERROR;

/** Manages the long-lived Python subprocess used to execute row-level transform logic. */
@Slf4j
class PythonProcessWorker {

    /** Shared object mapper used for the stdin/stdout row protocol. */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** Maximum number of stderr lines preserved in failure messages. */
    private static final int STDERR_TAIL_LIMIT = 20;

    /** Maximum number of unread protocol responses retained from the Python worker. */
    private static final int STDOUT_QUEUE_CAPACITY = 32;

    /** Maximum number of pending writes retained for the single-owner stdin pump. */
    private static final int STDIN_QUEUE_CAPACITY = 32;

    /**
     * Briefly waits for daemon pipe collectors without letting inherited descriptors block close.
     */
    private static final long COLLECTOR_JOIN_TIMEOUT_MILLIS = 100L;

    /** Default executable name used when no python runtime is configured explicitly. */
    private static final String DEFAULT_PYTHON_EXECUTABLE = "python3";

    /** Immutable transform configuration shared by all processed rows. */
    private final PythonTransformConfig transformConfig;

    /** Input row schema used to serialize SeaTunnel rows into JSON objects. */
    private final SeaTunnelRowType inputRowType;

    /** Declared output columns used to interpret Python results. */
    private final List<PythonColumnConfig> outputColumnConfigs;

    /** JSON serializer for the input row. */
    private final RowToJsonConverters.RowToJsonConverter rowToJsonConverter;

    /** Type-aware converters used to rebuild SeaTunnel field values from JSON. */
    private final JsonToRowConverters.JsonToObjectConverter[] outputConverters;

    /** Recent stderr lines from the Python worker to enrich failure messages. */
    private final ArrayDeque<String> stderrTail = new ArrayDeque<>(STDERR_TAIL_LIMIT);

    /** Coordinates resource publication with asynchronous task cancellation. */
    private final Object lifecycleLock = new Object();

    /** Atomically orders stdin request admission, pump ownership, and terminal shutdown. */
    private final Object stdinLifecycleLock = new Object();

    /** Responses read by a daemon thread so task interruption never blocks in native pipe IO. */
    private final BlockingQueue<WorkerOutput> stdoutMessages =
            new ArrayBlockingQueue<>(STDOUT_QUEUE_CAPACITY);

    /** Requests handled by one daemon writer so task and close threads never enter pipe writes. */
    private final BlockingQueue<StdinRequest> stdinRequests =
            new ArrayBlockingQueue<>(STDIN_QUEUE_CAPACITY);

    /** True only while new protocol writes may be admitted to the current stdin pump. */
    private boolean stdinAccepting;

    /** Request currently owned by the stdin pump, completed exceptionally on forced shutdown. */
    private StdinRequest activeStdinRequest;

    /** Running Python process instance. */
    private volatile Process process;

    /** Writer connected to the Python worker stdin. */
    private volatile BufferedWriter stdinWriter;

    /** Raw stdin pipe released only after its single writer thread has stopped. */
    private volatile OutputStream stdinStream;

    /** Single owner of stdin pipe writes and graceful EOF signaling. */
    private volatile Thread stdinWriterThread;

    /** Reader connected to the Python worker stdout. */
    private volatile BufferedReader stdoutReader;

    /** Raw stdout pipe closed synchronously only after its collector has stopped. */
    private volatile InputStream stdoutStream;

    /** Reader connected to the Python worker stderr. */
    private volatile BufferedReader stderrReader;

    /** Raw stderr pipe closed synchronously only after its collector has stopped. */
    private volatile InputStream stderrStream;

    /** Background stderr collector that preserves debug context without polluting stdout. */
    private volatile Thread stderrCollectorThread;

    /** Background stdout collector that transfers process responses to an interruptible queue. */
    private volatile Thread stdoutCollectorThread;

    /** Temporary worker bootstrap script materialized from the classpath resource. */
    private volatile Path workerScriptPath;

    /** Temporary file used when the user provides inline Python code. */
    private volatile Path inlineSourceCodePath;

    /** Prevents a row request from reopening the subprocess after transform cancellation. */
    private volatile boolean closed;

    /** Monotonic request id used to correlate sequential row requests. */
    private long requestId;

    /**
     * Creates a process worker bound to one catalog table schema.
     *
     * @param transformConfig normalized transform configuration
     * @param catalogTable input table used to derive row serialization and output schema
     */
    PythonProcessWorker(PythonTransformConfig transformConfig, CatalogTable catalogTable) {
        this.transformConfig = transformConfig;
        this.inputRowType = catalogTable.getSeaTunnelRowType();
        this.outputColumnConfigs = transformConfig.getColumnConfigs();
        this.rowToJsonConverter = new RowToJsonConverters().createConverter(inputRowType, null);
        JsonToRowConverters jsonToRowConverters = new JsonToRowConverters(false, false);
        this.outputConverters =
                outputColumnConfigs.stream()
                        .map(PythonColumnConfig::getDestColumn)
                        .map(Column::getDataType)
                        .map(jsonToRowConverters::createConverter)
                        .toArray(JsonToRowConverters.JsonToObjectConverter[]::new);
    }

    /**
     * Starts the Python worker lazily so schema planning does not require a local Python runtime.
     */
    synchronized void open() {
        ensureNotClosed();
        if (process != null) {
            if (process.isAlive()) {
                return;
            }
            String failureMessage =
                    buildWorkerFailureMessage(
                            "Python worker exited and cannot be restarted safely");
            closeSilently();
            throw new TransformException(PYTHON_PROCESS_TERMINATED_ERROR, failureMessage);
        }
        Path currentWorkerScriptPath = null;
        Path currentInlineSourceCodePath = null;
        Process currentProcess = null;
        OutputStream currentStdinStream = null;
        BufferedWriter currentStdinWriter = null;
        InputStream currentStdoutStream = null;
        BufferedReader currentStdoutReader = null;
        InputStream currentStderrStream = null;
        BufferedReader currentStderrReader = null;
        Thread currentStdinWriterThread = null;
        Thread currentStdoutCollectorThread = null;
        Thread currentStderrCollectorThread = null;
        boolean resourcesPublished = false;
        try {
            synchronized (stderrTail) {
                stderrTail.clear();
            }
            synchronized (stdinLifecycleLock) {
                stdinRequests.clear();
                activeStdinRequest = null;
                stdinAccepting = true;
            }
            stdoutMessages.clear();
            currentWorkerScriptPath = writeWorkerScript();
            Path userScriptPath = resolveUserScriptPath();
            if (transformConfig.getSourceCode() != null) {
                currentInlineSourceCodePath = userScriptPath;
            }
            currentProcess = startPythonProcess(currentWorkerScriptPath, userScriptPath);
            currentStdinStream = currentProcess.getOutputStream();
            currentStdinWriter =
                    new BufferedWriter(
                            new OutputStreamWriter(currentStdinStream, StandardCharsets.UTF_8));
            currentStdoutStream = currentProcess.getInputStream();
            currentStdoutReader =
                    new BufferedReader(
                            new InputStreamReader(currentStdoutStream, StandardCharsets.UTF_8));
            currentStderrStream = currentProcess.getErrorStream();
            currentStderrReader =
                    new BufferedReader(
                            new InputStreamReader(currentStderrStream, StandardCharsets.UTF_8));
            currentStdinWriterThread = createStdinWriter(currentStdinWriter);
            currentStdoutCollectorThread = createStdoutCollector(currentStdoutReader);
            currentStderrCollectorThread = createStderrCollector(currentStderrReader);
            synchronized (lifecycleLock) {
                ensureNotClosed();
                workerScriptPath = currentWorkerScriptPath;
                inlineSourceCodePath = currentInlineSourceCodePath;
                process = currentProcess;
                stdinStream = currentStdinStream;
                stdinWriter = currentStdinWriter;
                stdinWriterThread = currentStdinWriterThread;
                stdoutStream = currentStdoutStream;
                stdoutReader = currentStdoutReader;
                stderrStream = currentStderrStream;
                stderrReader = currentStderrReader;
                stdoutCollectorThread = currentStdoutCollectorThread;
                stderrCollectorThread = currentStderrCollectorThread;
                resourcesPublished = true;
                currentStdinWriterThread.start();
                currentStdoutCollectorThread.start();
                currentStderrCollectorThread.start();
            }
            initializeRemoteContext();
            ensureNotClosed();
        } catch (IOException e) {
            closeSilently();
            if (!resourcesPublished) {
                terminateWorker(
                        currentProcess,
                        currentStdinStream,
                        currentStdinWriter,
                        currentStdoutStream,
                        currentStdoutReader,
                        currentStderrStream,
                        currentStderrReader,
                        currentStdinWriterThread,
                        currentStdoutCollectorThread,
                        currentStderrCollectorThread,
                        currentWorkerScriptPath,
                        currentInlineSourceCodePath,
                        false);
            }
            throw new TransformException(
                    START_PYTHON_PROCESS_ERROR,
                    START_PYTHON_PROCESS_ERROR.getDescription() + ": " + e.getMessage());
        } catch (RuntimeException e) {
            closeSilently();
            if (!resourcesPublished) {
                terminateWorker(
                        currentProcess,
                        currentStdinStream,
                        currentStdinWriter,
                        currentStdoutStream,
                        currentStdoutReader,
                        currentStderrStream,
                        currentStderrReader,
                        currentStdinWriterThread,
                        currentStdoutCollectorThread,
                        currentStderrCollectorThread,
                        currentWorkerScriptPath,
                        currentInlineSourceCodePath,
                        false);
            }
            throw e;
        }
    }

    /**
     * Sends one logical row to the Python worker and converts the JSON response back into
     * SeaTunnel-compatible typed values using the configured output column list.
     */
    synchronized Object[] processRow(SeaTunnelRowAccessor inputRow) {
        open();
        ensureNotClosed();
        BufferedReader currentStdoutReader = stdoutReader;
        if (stdinWriterThread == null || currentStdoutReader == null) {
            throw new TransformException(
                    PYTHON_PROCESS_TERMINATED_ERROR,
                    buildWorkerFailureMessage("Python worker is not available"));
        }
        ObjectNode requestNode = OBJECT_MAPPER.createObjectNode();
        long currentRequestId = ++requestId;
        requestNode.put("id", currentRequestId);
        requestNode.set("row", toRowJson(inputRow));
        try {
            writeWorkerMessage(OBJECT_MAPPER.writeValueAsString(requestNode));

            String responseLine =
                    takeWorkerOutput("Python worker exited before returning a response");
            JsonNode response = parseWorkerResponse(responseLine);
            validateResponse(response, currentRequestId);
            if (response.hasNonNull("error")) {
                return handlePythonExecutionError(response.get("error").asText());
            }
            if (!response.has("result")) {
                closeSilently();
                throw new TransformException(
                        INVALID_PYTHON_RESULT_ERROR,
                        INVALID_PYTHON_RESULT_ERROR.getDescription()
                                + ": missing 'result' field in worker response");
            }
            return convertResult(response.get("result"));
        } catch (IOException e) {
            closeSilently();
            throw new TransformException(
                    PYTHON_PROCESS_TERMINATED_ERROR, buildWorkerFailureMessage(e.getMessage()));
        }
    }

    /**
     * Terminates the Python process and unblocks a concurrent row request during task cancellation.
     */
    void close() {
        closeInternal(true);
    }

    /** Closes a failed or cancelled worker without replacing the primary exception. */
    private void closeSilently() {
        closeInternal(false);
    }

    /** Atomically detaches and terminates the current resource snapshot. */
    private void closeInternal(boolean reportCloseFailure) {
        closed = true;

        BufferedWriter currentStdinWriter;
        OutputStream currentStdinStream;
        InputStream currentStdoutStream;
        BufferedReader currentStdoutReader;
        InputStream currentStderrStream;
        BufferedReader currentStderrReader;
        Process currentProcess;
        Thread currentStdinWriterThread;
        Thread currentStdoutCollectorThread;
        Thread currentStderrCollectorThread;
        Path currentWorkerScriptPath;
        Path currentInlineSourceCodePath;
        synchronized (lifecycleLock) {
            currentStdinStream = stdinStream;
            currentStdinWriter = stdinWriter;
            currentStdoutStream = stdoutStream;
            currentStdoutReader = stdoutReader;
            currentStderrStream = stderrStream;
            currentStderrReader = stderrReader;
            currentProcess = process;
            currentStdinWriterThread = stdinWriterThread;
            currentStdoutCollectorThread = stdoutCollectorThread;
            currentStderrCollectorThread = stderrCollectorThread;
            currentWorkerScriptPath = workerScriptPath;
            currentInlineSourceCodePath = inlineSourceCodePath;

            process = null;
            stdinStream = null;
            stdinWriter = null;
            stdoutStream = null;
            stdoutReader = null;
            stderrStream = null;
            stderrReader = null;
            stdinWriterThread = null;
            stdoutCollectorThread = null;
            stderrCollectorThread = null;
            workerScriptPath = null;
            inlineSourceCodePath = null;
        }
        publishStdoutTerminal(new IOException("Python worker closed during shutdown"));
        TransformException closeFailure =
                terminateWorker(
                        currentProcess,
                        currentStdinStream,
                        currentStdinWriter,
                        currentStdoutStream,
                        currentStdoutReader,
                        currentStderrStream,
                        currentStderrReader,
                        currentStdinWriterThread,
                        currentStdoutCollectorThread,
                        currentStderrCollectorThread,
                        currentWorkerScriptPath,
                        currentInlineSourceCodePath,
                        reportCloseFailure);
        synchronized (stderrTail) {
            stderrTail.clear();
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    /** Fails fast when cancellation has permanently closed this worker instance. */
    private void ensureNotClosed() {
        if (!closed) {
            return;
        }
        throw new TransformException(
                PYTHON_PROCESS_TERMINATED_ERROR,
                buildWorkerFailureMessage("Python worker has been closed"));
    }

    /**
     * Converts Python-side failures into the common SeaTunnel row error handling contract.
     *
     * @param errorMessage python traceback or user error
     * @return never returns normally because the transform layer handles the thrown exception
     */
    private Object[] handlePythonExecutionError(String errorMessage) {
        ErrorHandleWay errorHandleWay = transformConfig.getErrorHandleWay();
        String detailedMessage = buildWorkerFailureMessage(errorMessage);
        if (errorHandleWay != null && errorHandleWay.allowSkipThisRow()) {
            throw new ErrorDataTransformException(
                    errorHandleWay, PYTHON_EXECUTION_ERROR, detailedMessage);
        }
        throw new ErrorDataTransformException(PYTHON_EXECUTION_ERROR, detailedMessage);
    }

    /** Rejects malformed or out-of-order responses before they can desynchronize later rows. */
    private void validateResponse(JsonNode response, long expectedRequestId) {
        if (response != null
                && response.isObject()
                && response.hasNonNull("id")
                && response.get("id").isIntegralNumber()
                && response.get("id").asLong() == expectedRequestId) {
            return;
        }
        String actualResponse = response == null ? "null" : response.toString();
        closeSilently();
        throw new TransformException(
                INVALID_PYTHON_RESULT_ERROR,
                INVALID_PYTHON_RESULT_ERROR.getDescription()
                        + ": expected response id "
                        + expectedRequestId
                        + " but received "
                        + actualResponse);
    }

    /** Parses one protocol line and poisons the worker when stdout is not valid JSON. */
    private JsonNode parseWorkerResponse(String responseLine) {
        try {
            return JsonUtils.stringToJsonNode(responseLine);
        } catch (IOException | RuntimeException e) {
            closeSilently();
            throw new TransformException(
                    INVALID_PYTHON_RESULT_ERROR,
                    INVALID_PYTHON_RESULT_ERROR.getDescription()
                            + ": invalid worker response: "
                            + responseLine);
        }
    }

    /**
     * Converts one Python result payload into the declared output field values.
     *
     * @param resultNode python result payload
     * @return typed output values aligned with the configured columns
     */
    private Object[] convertResult(JsonNode resultNode) {
        if (resultNode == null || resultNode.isNull()) {
            return new Object[outputConverters.length];
        }
        if (resultNode.isArray()) {
            return convertArrayResult((ArrayNode) resultNode);
        }
        if (resultNode.isObject()) {
            return convertObjectResult((ObjectNode) resultNode);
        }
        if (outputConverters.length == 1) {
            return new Object[] {
                outputConverters[0].convert(resultNode, outputColumnConfigs.get(0).getDestField())
            };
        }
        throw new TransformException(
                INVALID_PYTHON_RESULT_ERROR,
                INVALID_PYTHON_RESULT_ERROR.getDescription()
                        + ": expected an array or object when multiple columns are configured");
    }

    /**
     * Maps an ordered Python array result to the configured output columns.
     *
     * @param resultArray ordered python result array
     * @return typed output values
     */
    private Object[] convertArrayResult(ArrayNode resultArray) {
        if (resultArray.size() != outputConverters.length) {
            throw new TransformException(
                    INVALID_PYTHON_RESULT_ERROR,
                    INVALID_PYTHON_RESULT_ERROR.getDescription()
                            + ": expected "
                            + outputConverters.length
                            + " values but got "
                            + resultArray.size());
        }
        Object[] values = new Object[outputConverters.length];
        for (int i = 0; i < outputConverters.length; i++) {
            values[i] =
                    outputConverters[i].convert(
                            resultArray.get(i), outputColumnConfigs.get(i).getDestField());
        }
        return values;
    }

    /**
     * Maps a Python object result to the configured output columns by field name.
     *
     * @param resultObject python result object
     * @return typed output values
     */
    private Object[] convertObjectResult(ObjectNode resultObject) {
        Object[] values = new Object[outputConverters.length];
        for (int i = 0; i < outputConverters.length; i++) {
            String destField = outputColumnConfigs.get(i).getDestField();
            if (!resultObject.has(destField)) {
                throw new TransformException(
                        INVALID_PYTHON_RESULT_ERROR,
                        INVALID_PYTHON_RESULT_ERROR.getDescription()
                                + ": missing declared field '"
                                + destField
                                + "'");
            }
            values[i] = outputConverters[i].convert(resultObject.get(destField), destField);
        }
        return values;
    }

    /**
     * Serializes one SeaTunnel row accessor into the JSON shape expected by the worker.
     *
     * @param inputRow current row accessor
     * @return json object keyed by input field name
     */
    private ObjectNode toRowJson(SeaTunnelRowAccessor inputRow) {
        SeaTunnelRow row = new SeaTunnelRow(inputRowType.getTotalFields());
        for (int i = 0; i < inputRowType.getTotalFields(); i++) {
            row.setField(i, inputRow.getField(i));
        }
        ObjectNode rowNode = OBJECT_MAPPER.createObjectNode();
        rowToJsonConverter.convert(OBJECT_MAPPER, rowNode, row);
        return rowNode;
    }

    /**
     * Sends schema metadata and static script config to the Python worker once per process.
     *
     * @throws IOException when the worker pipe cannot be used
     */
    private void initializeRemoteContext() throws IOException {
        ObjectNode initNode = OBJECT_MAPPER.createObjectNode();
        initNode.put("id", 0L);
        initNode.set("context", buildRuntimeContext());
        writeWorkerMessage(OBJECT_MAPPER.writeValueAsString(initNode));

        String responseLine = takeWorkerOutput("Python worker exited during initialization");
        JsonNode response = parseWorkerResponse(responseLine);
        validateResponse(response, 0L);
        if (!response.path("ok").asBoolean(false)) {
            String errorMessage = response.path("error").asText("unknown initialization error");
            throw new TransformException(
                    INIT_PYTHON_PROCESS_ERROR, buildWorkerFailureMessage(errorMessage));
        }
    }

    /** Queues one protocol line and waits interruptibly for the stdin pump to flush it. */
    private void writeWorkerMessage(String line) throws IOException {
        StdinRequest request = StdinRequest.line(line);
        String admissionFailure = null;
        synchronized (stdinLifecycleLock) {
            Thread currentWriterThread = stdinWriterThread;
            if (!stdinAccepting || currentWriterThread == null || !currentWriterThread.isAlive()) {
                admissionFailure = "Python worker stdin writer is not available";
            } else if (!stdinRequests.offer(request)) {
                admissionFailure = "Python worker stdin queue overflow";
            } else {
                stdinLifecycleLock.notifyAll();
            }
        }
        if (admissionFailure != null) {
            closeSilently();
            throw new IOException(admissionFailure);
        }
        try {
            request.await();
        } catch (InterruptedException e) {
            String message =
                    buildWorkerFailureMessage("Interrupted while writing to Python worker");
            Thread.currentThread().interrupt();
            closeSilently();
            throw new TransformException(PYTHON_PROCESS_TERMINATED_ERROR, message);
        }
        if (request.error != null) {
            throw request.error;
        }
    }

    /**
     * Builds the Python runtime context exposed to user scripts.
     *
     * @return context object containing schema metadata and static config
     */
    private ObjectNode buildRuntimeContext() {
        ObjectNode contextNode = OBJECT_MAPPER.createObjectNode();
        contextNode.set(
                "input_fields",
                buildFieldMetadata(inputRowType.getFieldNames(), inputRowType.getFieldTypes()));
        String[] outputFieldNames =
                outputColumnConfigs.stream()
                        .map(PythonColumnConfig::getDestField)
                        .toArray(String[]::new);
        contextNode.set(
                "output_fields",
                buildFieldMetadata(
                        outputFieldNames,
                        outputColumnConfigs.stream()
                                .map(PythonColumnConfig::getDestColumn)
                                .map(Column::getDataType)
                                .toArray(
                                        org.apache.seatunnel.api.table.type.SeaTunnelDataType[]
                                                ::new)));
        contextNode.set("config", OBJECT_MAPPER.valueToTree(transformConfig.getScriptConfig()));
        return contextNode;
    }

    /**
     * Converts SeaTunnel field metadata into a compact JSON array for the Python runtime.
     *
     * @param fieldNames field names in schema order
     * @param fieldTypes field types aligned with the names
     * @return array of field metadata objects
     */
    private ArrayNode buildFieldMetadata(
            String[] fieldNames,
            org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] fieldTypes) {
        ArrayNode fieldsNode = OBJECT_MAPPER.createArrayNode();
        for (int i = 0; i < fieldNames.length; i++) {
            ObjectNode fieldNode = fieldsNode.addObject();
            fieldNode.put("name", fieldNames[i]);
            fieldNode.put("type", fieldTypes[i].toString());
        }
        return fieldsNode;
    }

    /**
     * Starts the Python worker with the configured executable and user script path.
     *
     * @param workerPath bootstrap worker path
     * @param userScriptPath user script path visible to the runtime host
     * @return running python process
     * @throws IOException when all executable candidates fail
     */
    private Process startPythonProcess(Path workerPath, Path userScriptPath) throws IOException {
        List<String> candidates = resolvePythonCandidates(transformConfig.getPythonExecutable());
        IOException lastException = null;
        for (String candidate : candidates) {
            try {
                ProcessBuilder builder =
                        new ProcessBuilder(
                                candidate, workerPath.toString(), userScriptPath.toString());
                builder.redirectErrorStream(false);
                return builder.start();
            } catch (IOException e) {
                lastException = e;
            }
        }
        throw new IOException(
                START_PYTHON_PROCESS_ERROR.getDescription()
                        + ": "
                        + Arrays.toString(candidates.toArray())
                        + " -> "
                        + (lastException == null ? "unknown error" : lastException.getMessage()),
                lastException);
    }

    /**
     * Expands the configured python executable into fallback candidates.
     *
     * @param configuredExecutable user-configured python executable
     * @return ordered executable candidates
     */
    private List<String> resolvePythonCandidates(String configuredExecutable) {
        List<String> candidates = new ArrayList<>();
        candidates.add(configuredExecutable);
        if (DEFAULT_PYTHON_EXECUTABLE.equals(configuredExecutable)) {
            candidates.add("python");
        }
        return candidates;
    }

    /**
     * Resolves the user script path, materializing inline source code when necessary.
     *
     * @return local path passed to the python worker
     * @throws IOException when the inline source file cannot be written
     */
    private Path resolveUserScriptPath() throws IOException {
        if (transformConfig.getSourceCode() != null) {
            Path path = Files.createTempFile("seatunnel-python-transform-user", ".py");
            try {
                Files.write(path, transformConfig.getSourceCode().getBytes(StandardCharsets.UTF_8));
                return path;
            } catch (IOException | RuntimeException e) {
                deleteQuietly(path);
                throw e;
            }
        }
        return Paths.get(transformConfig.getSourceCodePath());
    }

    /**
     * Materializes the bundled bootstrap script into a temporary file.
     *
     * @return worker bootstrap script path
     * @throws IOException when the resource cannot be copied
     */
    private Path writeWorkerScript() throws IOException {
        Path path = Files.createTempFile("seatunnel-python-transform-worker", ".py");
        try {
            try (InputStream inputStream =
                    PythonProcessWorker.class
                            .getClassLoader()
                            .getResourceAsStream("python_transform/worker_template.py")) {
                if (inputStream == null) {
                    throw new TransformException(
                            LOAD_WORKER_SCRIPT_ERROR,
                            LOAD_WORKER_SCRIPT_ERROR.getDescription() + ": resource not found");
                }
                Files.copy(inputStream, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
            return path;
        } catch (IOException | RuntimeException e) {
            deleteQuietly(path);
            throw e;
        }
    }

    /** Creates the sole stdin pipe owner so lifecycle callers never block in native writes. */
    private Thread createStdinWriter(BufferedWriter writer) {
        Thread writerThread =
                new Thread(
                        () -> {
                            IOException terminalError = null;
                            try {
                                while (true) {
                                    StdinRequest request = takeNextStdinRequest();
                                    if (request == null) {
                                        return;
                                    }
                                    try {
                                        if (request.close) {
                                            writer.close();
                                            request.complete(null);
                                            return;
                                        }
                                        writer.write(request.line);
                                        writer.newLine();
                                        writer.flush();
                                        request.complete(null);
                                    } catch (IOException e) {
                                        terminalError = e;
                                        request.complete(e);
                                        return;
                                    } finally {
                                        if (!request.isCompleted()) {
                                            request.complete(
                                                    terminalError == null
                                                            ? new IOException(
                                                                    "Python stdin writer stopped")
                                                            : terminalError);
                                        }
                                        synchronized (stdinLifecycleLock) {
                                            if (activeStdinRequest == request) {
                                                activeStdinRequest = null;
                                            }
                                        }
                                    }
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                terminalError =
                                        new IOException("Python stdin writer interrupted", e);
                            } finally {
                                IOException finalError =
                                        terminalError == null
                                                ? new IOException("Python stdin writer stopped")
                                                : terminalError;
                                synchronized (stdinLifecycleLock) {
                                    stdinAccepting = false;
                                    if (activeStdinRequest != null) {
                                        activeStdinRequest.complete(finalError);
                                        activeStdinRequest = null;
                                    }
                                    StdinRequest pending;
                                    while ((pending = stdinRequests.poll()) != null) {
                                        pending.complete(finalError);
                                    }
                                    stdinLifecycleLock.notifyAll();
                                }
                            }
                        },
                        "seatunnel-python-transform-stdin");
        writerThread.setDaemon(true);
        return writerThread;
    }

    /** Atomically transfers one admitted request from the queue to the stdin pump. */
    private StdinRequest takeNextStdinRequest() throws InterruptedException {
        synchronized (stdinLifecycleLock) {
            while (stdinRequests.isEmpty() && stdinAccepting) {
                stdinLifecycleLock.wait();
            }
            StdinRequest request = stdinRequests.poll();
            activeStdinRequest = request;
            return request;
        }
    }

    /**
     * Starts a daemon thread that captures worker stderr for debugging without breaking stdout.
     *
     * @param errorReader stderr reader from the python process
     */
    private Thread createStderrCollector(BufferedReader errorReader) {
        Thread collectorThread =
                new Thread(
                        () -> {
                            try (BufferedReader reader = errorReader) {
                                String line;
                                while ((line = reader.readLine()) != null) {
                                    synchronized (stderrTail) {
                                        if (stderrTail.size() == STDERR_TAIL_LIMIT) {
                                            stderrTail.removeFirst();
                                        }
                                        stderrTail.addLast(line);
                                    }
                                    log.debug("Python transform stderr: {}", line);
                                }
                            } catch (IOException e) {
                                log.debug("Stop Python transform stderr collector", e);
                            }
                        },
                        "seatunnel-python-transform-stderr");
        collectorThread.setDaemon(true);
        return collectorThread;
    }

    /**
     * Reads stdout in a daemon thread and publishes protocol messages to an interruptible queue.
     */
    private Thread createStdoutCollector(BufferedReader outputReader) {
        Thread collectorThread =
                new Thread(
                        () -> {
                            try (BufferedReader reader = outputReader) {
                                String line;
                                while ((line = reader.readLine()) != null) {
                                    if (!stdoutMessages.offer(WorkerOutput.line(line))) {
                                        publishStdoutTerminal(
                                                new IOException(
                                                        "Python worker stdout queue overflow"));
                                        return;
                                    }
                                }
                                publishStdoutTerminal(null);
                            } catch (IOException e) {
                                publishStdoutTerminal(e);
                            }
                        },
                        "seatunnel-python-transform-stdout");
        collectorThread.setDaemon(true);
        return collectorThread;
    }

    /** Publishes a terminal stdout signal even when invalid script output filled the queue. */
    private void publishStdoutTerminal(IOException error) {
        if (stdoutMessages.offer(WorkerOutput.closed(error))) {
            return;
        }
        stdoutMessages.clear();
        stdoutMessages.offer(WorkerOutput.closed(error));
    }

    /** Waits for one protocol response while preserving task cancellation via interruption. */
    private String takeWorkerOutput(String closedMessage) {
        try {
            WorkerOutput output = stdoutMessages.take();
            if (output.line != null) {
                return output.line;
            }
            String message = closedMessage;
            if (output.error != null && output.error.getMessage() != null) {
                message += ": " + output.error.getMessage();
            }
            throw new TransformException(
                    PYTHON_PROCESS_TERMINATED_ERROR, buildWorkerFailureMessage(message));
        } catch (InterruptedException e) {
            String message =
                    buildWorkerFailureMessage("Interrupted while waiting for Python worker");
            Thread.currentThread().interrupt();
            closeSilently();
            throw new TransformException(PYTHON_PROCESS_TERMINATED_ERROR, message);
        }
    }

    /** Terminates one resource snapshot without consulting mutable worker fields. */
    private TransformException terminateWorker(
            Process currentProcess,
            OutputStream currentStdinStream,
            BufferedWriter currentStdinWriter,
            InputStream currentStdoutStream,
            BufferedReader currentStdoutReader,
            InputStream currentStderrStream,
            BufferedReader currentStderrReader,
            Thread currentStdinWriterThread,
            Thread currentStdoutCollectorThread,
            Thread currentStderrCollectorThread,
            Path currentWorkerScriptPath,
            Path currentInlineSourceCodePath,
            boolean reportCloseFailure) {
        boolean interrupted = Thread.interrupted();
        boolean stdinClosed = false;
        if (currentStdinWriterThread != null && currentStdinWriterThread.isAlive()) {
            StdinRequest closeRequest = StdinRequest.close();
            boolean closeAdmitted;
            synchronized (stdinLifecycleLock) {
                stdinAccepting = false;
                closeAdmitted = stdinRequests.offer(closeRequest);
                stdinLifecycleLock.notifyAll();
            }
            if (closeAdmitted) {
                try {
                    stdinClosed = closeRequest.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } else {
            failPendingStdinRequests(new IOException("Python stdin writer is not available"));
            closeQuietly(currentStdinWriter, "stdin writer");
            stdinClosed = true;
        }
        ProcessStopResult stopResult = ProcessStopResult.notStarted(interrupted);
        if (currentProcess != null) {
            stopResult =
                    waitForProcessToStop(currentProcess, !stdinClosed || interrupted, interrupted);
            interrupted = stopResult.interrupted;
        }
        interrupted |= waitForThreadToStop(currentStdinWriterThread, TimeUnit.SECONDS.toMillis(5));
        if (currentStdinWriterThread == null || !currentStdinWriterThread.isAlive()) {
            closeQuietly(currentStdinStream, "stdin stream");
            closeQuietly(currentStdinWriter, "stdin writer");
        } else {
            failPendingStdinRequests(
                    new IOException("Python stdin writer did not stop after process shutdown"));
            log.warn("Python stdin writer did not terminate after process shutdown");
        }
        // A detached child can retain inherited pipe handles. Closing either the raw stream or
        // BufferedReader from this thread can then block on Windows, so each collector owns its
        // reader and task shutdown only closes streams after that collector has stopped.
        interrupted |=
                waitForThreadToStop(currentStdoutCollectorThread, COLLECTOR_JOIN_TIMEOUT_MILLIS);
        interrupted |=
                waitForThreadToStop(currentStderrCollectorThread, COLLECTOR_JOIN_TIMEOUT_MILLIS);
        closeProcessOutputAfterCollectorStops(
                currentStdoutStream, currentStdoutReader, currentStdoutCollectorThread, "stdout");
        closeProcessOutputAfterCollectorStops(
                currentStderrStream, currentStderrReader, currentStderrCollectorThread, "stderr");
        deleteQuietly(currentWorkerScriptPath);
        deleteQuietly(currentInlineSourceCodePath);
        TransformException closeFailure = null;
        if (reportCloseFailure && currentProcess != null && !interrupted) {
            if (stopResult.forced) {
                closeFailure =
                        new TransformException(
                                PYTHON_PROCESS_TERMINATED_ERROR,
                                buildWorkerFailureMessage(
                                        "Python worker did not stop cleanly during close"));
            } else if (stopResult.exitCode != 0) {
                closeFailure =
                        new TransformException(
                                PYTHON_PROCESS_TERMINATED_ERROR,
                                buildWorkerFailureMessage(
                                        "Python worker close hook failed with exit code "
                                                + stopResult.exitCode));
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return closeFailure;
    }

    /** Stops further admission and completes every owned or queued stdin request exceptionally. */
    private void failPendingStdinRequests(IOException error) {
        synchronized (stdinLifecycleLock) {
            stdinAccepting = false;
            if (activeStdinRequest != null) {
                activeStdinRequest.complete(error);
                activeStdinRequest = null;
            }
            StdinRequest pending;
            while ((pending = stdinRequests.poll()) != null) {
                pending.complete(error);
            }
            stdinLifecycleLock.notifyAll();
        }
    }

    /** Stops the subprocess deterministically while preserving the caller's interrupt status. */
    private ProcessStopResult waitForProcessToStop(
            Process currentProcess, boolean forceShutdown, boolean interrupted) {
        boolean forced = forceShutdown;
        if (!forceShutdown) {
            boolean stopped = false;
            try {
                if (currentProcess.waitFor(5, TimeUnit.SECONDS)) {
                    stopped = true;
                }
            } catch (InterruptedException e) {
                interrupted = true;
                forced = true;
            }
            if (stopped) {
                return ProcessStopResult.stopped(interrupted, false, currentProcess.exitValue());
            }
            forced = true;
        }
        currentProcess.destroy();
        try {
            if (currentProcess.waitFor(5, TimeUnit.SECONDS)) {
                return ProcessStopResult.stopped(interrupted, forced, currentProcess.exitValue());
            }
        } catch (InterruptedException e) {
            interrupted = true;
        }
        currentProcess.destroyForcibly();
        try {
            if (!currentProcess.waitFor(5, TimeUnit.SECONDS)) {
                log.warn("Python worker did not terminate after forced shutdown");
            }
        } catch (InterruptedException e) {
            interrupted = true;
        }
        int exitCode = currentProcess.isAlive() ? Integer.MIN_VALUE : currentProcess.exitValue();
        return ProcessStopResult.stopped(interrupted, true, exitCode);
    }

    /**
     * Closes one worker pipe quietly because shutdown should preserve the primary transform result.
     *
     * @param closeable worker pipe or reader
     * @param streamName logical stream name used in debug logs
     */
    private void closeQuietly(Closeable closeable, String streamName) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException e) {
            log.debug("Ignore Python worker {} close failure", streamName, e);
        }
    }

    /** Waits up to the caller's lifecycle budget for a process I/O thread to terminate. */
    private boolean waitForThreadToStop(Thread thread, long timeoutMillis) {
        if (thread == null) {
            return false;
        }
        try {
            thread.join(timeoutMillis);
            return false;
        } catch (InterruptedException e) {
            return true;
        }
    }

    /** Avoids blocking on stream or reader locks held by a collector on an inherited pipe. */
    private void closeProcessOutputAfterCollectorStops(
            InputStream stream, BufferedReader reader, Thread collectorThread, String streamName) {
        if (collectorThread != null && collectorThread.isAlive()) {
            log.debug(
                    "Python {} collector is still waiting for an inherited process descriptor",
                    streamName);
            return;
        }
        closeQuietly(reader, streamName + " reader");
        closeQuietly(stream, streamName + " stream");
    }

    /**
     * Appends recent stderr output to the primary failure message when available.
     *
     * @param message primary failure message
     * @return enriched failure message with stderr tail
     */
    private String buildWorkerFailureMessage(String message) {
        StringBuilder builder = new StringBuilder(message);
        synchronized (stderrTail) {
            if (!stderrTail.isEmpty()) {
                builder.append(" | stderr: ").append(String.join(" || ", stderrTail));
            }
        }
        return builder.toString();
    }

    /**
     * Deletes temporary files without masking the original transform outcome.
     *
     * @param path temporary file path
     */
    private void deleteQuietly(Path path) {
        if (path == null) {
            return;
        }
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.debug("Ignore Python transform temp file cleanup failure: {}", path, e);
        }
    }

    /** One stdin protocol line or the graceful EOF request owned by the writer pump. */
    private static final class StdinRequest {
        private final String line;
        private final boolean close;
        private final CountDownLatch completed = new CountDownLatch(1);
        private volatile IOException error;

        private StdinRequest(String line, boolean close) {
            this.line = line;
            this.close = close;
        }

        private static StdinRequest line(String line) {
            return new StdinRequest(line, false);
        }

        private static StdinRequest close() {
            return new StdinRequest(null, true);
        }

        private synchronized void complete(IOException error) {
            if (completed.getCount() == 0) {
                return;
            }
            this.error = error;
            completed.countDown();
        }

        private void await() throws InterruptedException {
            completed.await();
        }

        private boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return completed.await(timeout, unit);
        }

        private boolean isCompleted() {
            return completed.getCount() == 0;
        }
    }

    /** Captures whether shutdown was graceful so close-hook failures are reported accurately. */
    private static final class ProcessStopResult {
        private final boolean interrupted;
        private final boolean forced;
        private final int exitCode;

        private ProcessStopResult(boolean interrupted, boolean forced, int exitCode) {
            this.interrupted = interrupted;
            this.forced = forced;
            this.exitCode = exitCode;
        }

        private static ProcessStopResult notStarted(boolean interrupted) {
            return new ProcessStopResult(interrupted, false, 0);
        }

        private static ProcessStopResult stopped(
                boolean interrupted, boolean forced, int exitCode) {
            return new ProcessStopResult(interrupted, forced, exitCode);
        }
    }

    /** One stdout protocol line or a terminal process-stream signal. */
    private static final class WorkerOutput {
        private final String line;
        private final IOException error;

        private WorkerOutput(String line, IOException error) {
            this.line = line;
            this.error = error;
        }

        private static WorkerOutput line(String line) {
            return new WorkerOutput(line, null);
        }

        private static WorkerOutput closed(IOException error) {
            return new WorkerOutput(null, error);
        }
    }
}
