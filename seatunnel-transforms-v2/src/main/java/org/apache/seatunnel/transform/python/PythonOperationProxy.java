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

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.exception.ErrorDataTransformException;
import org.apache.seatunnel.transform.exception.TransformException;
import py4j.GatewayServer;
import py4j.Py4JException;
import sun.misc.IOUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.JSON_PATH_COMPILE_ERROR;
import static org.apache.seatunnel.transform.python.PythonTransformErrorCode.START_PYTHON_PROCESS_ERROR;

@Slf4j
public class PythonOperationProxy implements RowOperation {

    private final PythonTransformConfig transformConfig;

    private CloseRemotePython remotePython;
    private GatewayServer javaServer;
    private final LinkedBlockingQueue<SeaTunnelRowAccessorWithThread> dataQueue = new LinkedBlockingQueue<>();

    private final Map<Long, CompletableFuture<Object[]>> outputDataMap = new ConcurrentHashMap<>();


    /**
     * this method wait remote python client register close callback
     * @param remotePython python process
     */
    public void registerCloseRemotePython(CloseRemotePython remotePython) {
        this.remotePython = remotePython;
    }


    /**
     * The python process calls this method to run the code
     * @return pythonCode
     */
    public String getPythonCode(){
        return this.transformConfig.getSourceCode();
    }

    private PythonOperationProxy(PythonTransformConfig transformConfig) {
        if (INSTANCE != null) {
            throw new RuntimeException("Please use newInstance() method for getting the single instance of this class.");
        }
        this.transformConfig = transformConfig;
    }

    private static volatile PythonOperationProxy INSTANCE;

    public static PythonOperationProxy newInstance(PythonTransformConfig transformConfig) {
        if (INSTANCE == null) {
            synchronized (PythonOperationProxy.class) {
                if (INSTANCE == null) {
                    Integer javaServerPort = transformConfig.getJavaServerPort();
                    PythonOperationProxy operationProxy = new PythonOperationProxy(transformConfig);
                    operationProxy.javaServer = new GatewayServer(operationProxy, javaServerPort);
                    //Start the java process and then start python
                    startPythonProcess(transformConfig);
                }
            }
        }
        return INSTANCE;
    }

    private static void startPythonProcess(PythonTransformConfig transformConfig) {
        //python_transform/code_template.txt
        try {
            InputStream is = PythonOperationProxy.class.getClassLoader()
                    .getResourceAsStream("python_transform/code_template.txt");
            if (is == null) {
                throw new FileNotFoundException("找不到资源文件");
            }
            String scriptContent;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                scriptContent = reader.lines().reduce("", (a, b) -> a + b + "\n");
            }
            File tempFile = File.createTempFile("script-seatunnel-python-transform", ".py");
            tempFile.deleteOnExit();
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(tempFile), "UTF-8"))) {
                writer.write(scriptContent);
            }
            ProcessBuilder pb = new ProcessBuilder("python", tempFile.getAbsolutePath());
            pb.redirectErrorStream(true);
            pb.start();
        }catch (Exception e){
            throw new TransformException(START_PYTHON_PROCESS_ERROR,e.getMessage());
        }
    }

    public void shutdown() {
        //1.shutdown remote python
        try {
            this.remotePython.shutdownNow();
            //The shutdown of the remote side will result in no further responses,
            //which would cause an error due to the command not being received.
            // This Py4JException should be ignored.
        } catch (Py4JException ignore) {}
        this.javaServer.shutdown(true);
    }


    public void putData(Long threadId, List<Object> dataList) {
        Object[] outputData = dataList.toArray(new Object[0]);
        CompletableFuture<Object[]> future = outputDataMap.get(threadId);
        future.complete(outputData);
    }


    public SeaTunnelRowAccessorWithThread waitGetNextData() {
        SeaTunnelRowAccessorWithThread data = null;
        try {
            data = dataQueue.take();
        } catch (InterruptedException e) {
            //TODO
        }
        return data;
    }

    public Object[] processData(long threadId,
                                SeaTunnelRowAccessor inputRow) {
        CompletableFuture<Object[]> future = new CompletableFuture<>();
        outputDataMap.put(threadId, future);
        dataQueue.add(new SeaTunnelRowAccessorWithThread(threadId, inputRow));
        try {
            Object[] objects = future.get();
            outputDataMap.remove(threadId);
            return objects;
        } catch (InterruptedException | ExecutionException e) {
            ErrorHandleWay rowErrorHandleWay = transformConfig.getErrorHandleWay();
            if (rowErrorHandleWay != null
                    && rowErrorHandleWay.allowSkip()) {
                log.debug(
                        "Python transform error, ignore error, config: {}, value: {}",
                        transformConfig.getColumnConfigs(),
                        inputRow.getFields(),
                        e);
                return null;
            }
            throw new ErrorDataTransformException(
                    rowErrorHandleWay,
                    JSON_PATH_COMPILE_ERROR,
                    String.format(
                            "Python transform error, config: %s, value: %s, error: %s",
                            this.transformConfig, Arrays.toString(inputRow.getFields()), e.getMessage()));
        }
    }
}
