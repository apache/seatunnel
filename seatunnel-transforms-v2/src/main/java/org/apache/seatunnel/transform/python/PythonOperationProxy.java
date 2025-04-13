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
import py4j.GatewayServer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class PythonOperationProxy implements RowOperation {

    private GatewayServer javaServer;
    private final Map<Long, SeaTunnelRowAccessor> inputDataMap = new ConcurrentHashMap<>();

    private final Map<Long, EndTagList> outputDataMap = new ConcurrentHashMap<>();
    private PythonOperationProxy(){
        if (INSTANCE != null) {
            throw new RuntimeException("Please use newInstance() method for getting the single instance of this class.");
        }
    }
    private static volatile PythonOperationProxy INSTANCE;
    public static PythonOperationProxy newInstance(Integer javaServerPort) {
        if (INSTANCE == null){
            synchronized (PythonOperationProxy.class){
                if (INSTANCE == null){
                    PythonOperationProxy operationProxy = new PythonOperationProxy();
                    operationProxy.javaServer = new GatewayServer(operationProxy,javaServerPort);
                }
            }
        }
        return INSTANCE;
    }

    public void shutdown(){
        this.javaServer.shutdown();
    }

    public void putThreadData(long threadId, SeaTunnelRowAccessor inputRow) {
        this.inputDataMap.put(threadId,inputRow);
    }

    public Object[] getOutputData(long threadId) {
        EndTagList endTagList = outputDataMap.get(threadId);
        while (endTagList == null || !endTagList.isEnd()){
            log.info("wait python process data");
        }
        return outputDataMap.get(threadId).getList().toArray(new Object[0]);
    }

    public void addData(Long threadId,Object obj){
        EndTagList array = this.outputDataMap.getOrDefault(threadId, new EndTagList());
        array.add(obj);
        this.outputDataMap.put(threadId,array);
    }

    public void addDataList(Long threadId,List<Object> dataList){
        EndTagList array = new EndTagList();
        this.outputDataMap.put(threadId,array);
    }


    public void end(Long threadId){
        EndTagList endTagList = this.outputDataMap.get(threadId);
        if (endTagList != null){
            endTagList.end();
        }
    }
}
