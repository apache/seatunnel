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

    /** Responses read by a daemon thread so task interruption never blocks in native pipe IO. */
    private final BlockingQueue<WorkerOutput> stdoutMessages =
            new ArrayBlockingQueue<>(STDOUT_QUEUE_CAPACITY);

    /** Running Python process instance. */
    private volatile Process process;

    /** Writer connected to the Python worker stdin. */
    private volatile BufferedWriter stdinWriter;

    /** Reader connected to the Python worker stdout. */
    private volatile BufferedReader stdoutReader;

    /** Reader connected to the Python worker stderr. */
    private volatile BufferedReader stderrReader;

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
        if (process != null && process.isAlive()) {
            return;
        }
        Path currentWorkerScriptPath = null;
        Path currentInlineSourceCodePath = null;
        Process currentProcess = null;
        BufferedWriter currentStdinWriter = null;
        BufferedReader currentStdoutReader = null;
        BufferedReader currentStderrReader = null;
        Thread currentStdoutCollectorThread = null;
        Thread currentStderrCollectorThread = null;
        boolean resourcesPublished = false;
        try {
            synchronized (stderrTail) {
                stderrTail.clear();
            }
            stdoutMessages.clear();
            currentWorkerScriptPath = writeWorkerScript();
            Path userScriptPath = resolveUserScriptPath();
            if (transformConfig.getSourceCode() != null) {
                currentInlineSourceCodePath = userScriptPath;
            }
            currentProcess = startPythonProcess(currentWorkerScriptPath, userScriptPath);
            currentStdinWriter =
                    new BufferedWriter(
                            new OutputStreamWriter(
                                    currentProcess.getOutputStream(), StandardCharsets.UTF_8));
            currentStdoutReader =
                    new BufferedReader(
                            new InputStreamReader(
                                    currentProcess.getInputStream(), StandardCharsets.UTF_8));
            currentStderrReader =
                    new BufferedReader(
                            new InputStreamReader(
                                    currentProcess.getErrorStream(), StandardCharsets.UTF_8));
            currentStdoutCollectorThread = createStdoutCollector(currentStdoutReader);
            currentStderrCollectorThread = createStderrCollector(currentStderrReader);
            synchronized (lifecycleLock) {
                ensureNotClosed();
                workerScriptPath = currentWorkerScriptPath;
                inlineSourceCodePath = currentInlineSourceCodePath;
                process = currentProcess;
                stdinWriter = currentStdinWriter;
                stdoutReader = currentStdoutReader;
                stderrReader = currentStderrReader;
                stdoutCollectorThread = currentStdoutCollectorThread;
                stderrCollectorThread = currentStderrCollectorThread;
                resourcesPublished = true;
                currentStdoutCollectorThread.start();
                currentStderrCollectorThread.start();
            }
            initializeRemoteContext(currentStdinWriter);
            ensureNotClosed();
        } catch (IOException e) {
            close();
            if (!resourcesPublished) {
                terminateWorker(
                        currentProcess,
                        currentStdinWriter,
                        currentStdoutReader,
                        currentStderrReader,
                        currentStdoutCollectorThread,
                        currentStderrCollectorThread,
                        currentWorkerScriptPath,
                        currentInlineSourceCodePath);
            }
            throw new TransformException(
                    START_PYTHON_PROCESS_ERROR,
                    START_PYTHON_PROCESS_ERROR.getDescription() + ": " + e.getMessage());
        } catch (RuntimeException e) {
            close();
            if (!resourcesPublished) {
                terminateWorker(
                        currentProcess,
                        currentStdinWriter,
                        currentStdoutReader,
                        currentStderrReader,
                        currentStdoutCollectorThread,
                        currentStderrCollectorThread,
                        currentWorkerScriptPath,
                        currentInlineSourceCodePath);
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
        BufferedWriter currentStdinWriter = stdinWriter;
        BufferedReader currentStdoutReader = stdoutReader;
        if (currentStdinWriter == null || currentStdoutReader == null) {
            throw new TransformException(
                    PYTHON_PROCESS_TERMINATED_ERROR,
                    buildWorkerFailureMessage("Python worker is not available"));
        }
        ObjectNode requestNode = OBJECT_MAPPER.createObjectNode();
        requestNode.put("id", ++requestId);
        requestNode.set("row", toRowJson(inputRow));
        try {
            currentStdinWriter.write(OBJECT_MAPPER.writeValueAsString(requestNode));
            currentStdinWriter.newLine();
            currentStdinWriter.flush();

            String responseLine =
                    takeWorkerOutput("Python worker exited before returning a response");
            JsonNode response = JsonUtils.stringToJsonNode(responseLine);
            if (response.hasNonNull("error")) {
                return handlePythonExecutionError(response.get("error").asText());
            }
            if (!response.has("result")) {
                throw new TransformException(
                        INVALID_PYTHON_RESULT_ERROR,
                        INVALID_PYTHON_RESULT_ERROR.getDescription()
                                + ": missing 'result' field in worker response");
            }
            return convertResult(response.get("result"));
        } catch (IOException e) {
            throw new TransformException(
                    PYTHON_PROCESS_TERMINATED_ERROR, buildWorkerFailureMessage(e.getMessage()));
        }
    }

    /**
     * Terminates the Python process and unblocks a concurrent row request during task cancellation.
     */
    void close() {
        closed = true;

        BufferedWriter currentStdinWriter;
        BufferedReader currentStdoutReader;
        BufferedReader currentStderrReader;
        Process currentProcess;
        Thread currentStdoutCollectorThread;
        Thread currentStderrCollectorThread;
        Path currentWorkerScriptPath;
        Path currentInlineSourceCodePath;
        synchronized (lifecycleLock) {
            currentStdinWriter = stdinWriter;
            currentStdoutReader = stdoutReader;
            currentStderrReader = stderrReader;
            currentProcess = process;
            currentStdoutCollectorThread = stdoutCollectorThread;
            currentStderrCollectorThread = stderrCollectorThread;
            currentWorkerScriptPath = workerScriptPath;
            currentInlineSourceCodePath = inlineSourceCodePath;

            process = null;
            stdinWriter = null;
            stdoutReader = null;
            stderrReader = null;
            stdoutCollectorThread = null;
            stderrCollectorThread = null;
            workerScriptPath = null;
            inlineSourceCodePath = null;
        }
        terminateWorker(
                currentProcess,
                currentStdinWriter,
                currentStdoutReader,
                currentStderrReader,
                currentStdoutCollectorThread,
                currentStderrCollectorThread,
                currentWorkerScriptPath,
                currentInlineSourceCodePath);
        synchronized (stderrTail) {
            stderrTail.clear();
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
    private void initializeRemoteContext(BufferedWriter currentStdinWriter) throws IOException {
        ObjectNode initNode = OBJECT_MAPPER.createObjectNode();
        initNode.set("context", buildRuntimeContext());
        currentStdinWriter.write(OBJECT_MAPPER.writeValueAsString(initNode));
        currentStdinWriter.newLine();
        currentStdinWriter.flush();

        String responseLine = takeWorkerOutput("Python worker exited during initialization");
        JsonNode response = JsonUtils.stringToJsonNode(responseLine);
        if (!response.path("ok").asBoolean(false)) {
            String errorMessage = response.path("error").asText("unknown initialization error");
            throw new TransformException(
                    INIT_PYTHON_PROCESS_ERROR, buildWorkerFailureMessage(errorMessage));
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

    /**
     * Starts a daemon thread that captures worker stderr for debugging without breaking stdout.
     *
     * @param errorReader stderr reader from the python process
     */
    private Thread createStderrCollector(BufferedReader errorReader) {
        Thread collectorThread =
                new Thread(
                        () -> {
                            try {
                                String line;
                                while ((line = errorReader.readLine()) != null) {
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
                            try {
                                String line;
                                while ((line = outputReader.readLine()) != null) {
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
            close();
            throw new TransformException(PYTHON_PROCESS_TERMINATED_ERROR, message);
        }
    }

    /** Terminates one resource snapshot without consulting mutable worker fields. */
    private void terminateWorker(
            Process currentProcess,
            BufferedWriter currentStdinWriter,
            BufferedReader currentStdoutReader,
            BufferedReader currentStderrReader,
            Thread currentStdoutCollectorThread,
            Thread currentStderrCollectorThread,
            Path currentWorkerScriptPath,
            Path currentInlineSourceCodePath) {
        boolean interrupted = Thread.interrupted();
        // EOF is the normal shutdown signal and lets worker_template.py invoke the user close hook.
        closeQuietly(currentStdinWriter, "stdin");
        if (currentProcess != null) {
            interrupted |= waitForProcessToStop(currentProcess, interrupted);
        }
        closeQuietly(currentStdoutReader, "stdout");
        closeQuietly(currentStderrReader, "stderr");
        interrupted |= waitForCollectorToStop(currentStdoutCollectorThread);
        interrupted |= waitForCollectorToStop(currentStderrCollectorThread);
        deleteQuietly(currentWorkerScriptPath);
        deleteQuietly(currentInlineSourceCodePath);
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Stops the subprocess deterministically while preserving the caller's interrupt status. */
    private boolean waitForProcessToStop(Process currentProcess, boolean forceShutdown) {
        boolean interrupted = false;
        if (!forceShutdown) {
            boolean stopped = false;
            try {
                if (currentProcess.waitFor(5, TimeUnit.SECONDS)) {
                    stopped = true;
                }
            } catch (InterruptedException e) {
                interrupted = true;
            }
            if (stopped) {
                return interrupted;
            }
        }
        currentProcess.destroy();
        try {
            if (currentProcess.waitFor(5, TimeUnit.SECONDS)) {
                return interrupted;
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
        return interrupted;
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

    /** Waits for one daemon collector to release its process stream. */
    private boolean waitForCollectorToStop(Thread collectorThread) {
        if (collectorThread == null) {
            return false;
        }
        try {
            collectorThread.join(TimeUnit.SECONDS.toMillis(5));
            return false;
        } catch (InterruptedException e) {
            return true;
        }
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
