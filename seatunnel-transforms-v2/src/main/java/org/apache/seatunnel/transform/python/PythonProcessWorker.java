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

    /** Running Python process instance. */
    private Process process;

    /** Writer connected to the Python worker stdin. */
    private BufferedWriter stdinWriter;

    /** Reader connected to the Python worker stdout. */
    private BufferedReader stdoutReader;

    /** Reader connected to the Python worker stderr. */
    private BufferedReader stderrReader;

    /** Background stderr collector that preserves debug context without polluting stdout. */
    private Thread stderrCollectorThread;

    /** Temporary worker bootstrap script materialized from the classpath resource. */
    private Path workerScriptPath;

    /** Temporary file used when the user provides inline Python code. */
    private Path inlineSourceCodePath;

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
        if (process != null && process.isAlive()) {
            return;
        }
        try {
            stderrTail.clear();
            workerScriptPath = writeWorkerScript();
            Path userScriptPath = resolveUserScriptPath();
            process = startPythonProcess(workerScriptPath, userScriptPath);
            stdinWriter =
                    new BufferedWriter(
                            new OutputStreamWriter(
                                    process.getOutputStream(), StandardCharsets.UTF_8));
            stdoutReader =
                    new BufferedReader(
                            new InputStreamReader(
                                    process.getInputStream(), StandardCharsets.UTF_8));
            stderrReader =
                    new BufferedReader(
                            new InputStreamReader(
                                    process.getErrorStream(), StandardCharsets.UTF_8));
            startStderrCollector(stderrReader);
            initializeRemoteContext();
        } catch (IOException e) {
            close();
            throw new TransformException(
                    START_PYTHON_PROCESS_ERROR,
                    START_PYTHON_PROCESS_ERROR.getDescription() + ": " + e.getMessage());
        }
    }

    /**
     * Sends one logical row to the Python worker and converts the JSON response back into
     * SeaTunnel-compatible typed values using the configured output column list.
     */
    synchronized Object[] processRow(SeaTunnelRowAccessor inputRow) {
        open();
        ObjectNode requestNode = OBJECT_MAPPER.createObjectNode();
        requestNode.put("id", ++requestId);
        requestNode.set("row", toRowJson(inputRow));
        try {
            stdinWriter.write(OBJECT_MAPPER.writeValueAsString(requestNode));
            stdinWriter.newLine();
            stdinWriter.flush();

            String responseLine = stdoutReader.readLine();
            if (responseLine == null) {
                throw new TransformException(
                        PYTHON_PROCESS_TERMINATED_ERROR,
                        buildWorkerFailureMessage(
                                "Python worker exited before returning a response"));
            }
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

    synchronized void close() {
        closeQuietly(stdinWriter, "stdin");

        if (process != null) {
            try {
                if (!process.waitFor(5, TimeUnit.SECONDS)) {
                    process.destroy();
                    if (!process.waitFor(5, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                process.destroyForcibly();
            }
        }
        closeQuietly(stdoutReader, "stdout");
        closeQuietly(stderrReader, "stderr");
        waitForStderrCollectorToStop();

        deleteQuietly(workerScriptPath);
        deleteQuietly(inlineSourceCodePath);
        stderrTail.clear();

        process = null;
        stdinWriter = null;
        stdoutReader = null;
        stderrReader = null;
        stderrCollectorThread = null;
        workerScriptPath = null;
        inlineSourceCodePath = null;
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
    private void initializeRemoteContext() throws IOException {
        ObjectNode initNode = OBJECT_MAPPER.createObjectNode();
        initNode.set("context", buildRuntimeContext());
        stdinWriter.write(OBJECT_MAPPER.writeValueAsString(initNode));
        stdinWriter.newLine();
        stdinWriter.flush();

        String responseLine = stdoutReader.readLine();
        if (responseLine == null) {
            throw new TransformException(
                    INIT_PYTHON_PROCESS_ERROR,
                    buildWorkerFailureMessage("Python worker exited during initialization"));
        }
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
            inlineSourceCodePath = Files.createTempFile("seatunnel-python-transform-user", ".py");
            Files.write(
                    inlineSourceCodePath,
                    transformConfig.getSourceCode().getBytes(StandardCharsets.UTF_8));
            return inlineSourceCodePath;
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
    }

    /**
     * Starts a daemon thread that captures worker stderr for debugging without breaking stdout.
     *
     * @param errorReader stderr reader from the python process
     */
    private void startStderrCollector(BufferedReader errorReader) {
        stderrCollectorThread =
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
        stderrCollectorThread.setDaemon(true);
        stderrCollectorThread.start();
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

    /**
     * Waits for the background stderr collector to finish so the engine can recycle the classloader
     * without leaving an application thread behind.
     */
    private void waitForStderrCollectorToStop() {
        if (stderrCollectorThread == null) {
            return;
        }
        try {
            stderrCollectorThread.join(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
}
