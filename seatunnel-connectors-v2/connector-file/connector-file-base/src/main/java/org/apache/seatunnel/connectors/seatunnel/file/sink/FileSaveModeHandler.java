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

package org.apache.seatunnel.connectors.seatunnel.file.sink;

import org.apache.hadoop.fs.FileStatus;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

import java.io.FileNotFoundException;
import java.io.IOException;

public class FileSaveModeHandler implements SaveModeHandler {

    private final DataSaveMode dataSaveMode;

    private final SchemaSaveMode schemaSaveMode;
    private final HadoopFileSystemProxy  hadoopFileSystemProxy;

    protected final String path;

    private boolean emptyDir =false;

    public FileSaveModeHandler(HadoopConf hadoopConf, String path,
                               SchemaSaveMode schemaSaveMode,
                               DataSaveMode dataSaveMode) {

        this.hadoopFileSystemProxy=new HadoopFileSystemProxy(hadoopConf);
        this.dataSaveMode = dataSaveMode;
        this.path=path;
        this.schemaSaveMode = schemaSaveMode;
    }


    @Override
    public void handleSchemaSaveMode() {
        try {
            switch (schemaSaveMode) {
                case RECREATE_SCHEMA: {
                    recreateDir();
                    break;
                }case CREATE_SCHEMA_WHEN_NOT_EXIST:{
                    createWhenNotExists();break;
                }case ERROR_WHEN_SCHEMA_NOT_EXIST:{
                    errorWhenNotExist();break;
                }default:
                    throw new UnsupportedOperationException("Unsupported save mode: " + dataSaveMode);
            }
        }catch (IOException e){
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_READ_FAILED, e);
        }
    }

    @Override
    public void handleDataSaveMode() {
        if(emptyDir==true) return;
        try {

            switch (dataSaveMode) {
                case DROP_DATA: {
                    dropFile();
                    break;
                }
                case ERROR_WHEN_DATA_EXISTS: {
                    errorWhenDataExists();
                    break;
                }
            }
        }catch (IOException e){
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_READ_FAILED, e);
        }
    }

    @Override
    public void close() throws Exception {
        hadoopFileSystemProxy.close();
    }


    protected void recreateDir() throws IOException{
        if(hadoopFileSystemProxy.fileExist(path)){
            hadoopFileSystemProxy.deleteFile(path);
        }else{
            emptyDir =true;
        }
        hadoopFileSystemProxy.createDir(path);
    }


    protected void createWhenNotExists() throws IOException{
        if(!hadoopFileSystemProxy.fileExist(path)){
            hadoopFileSystemProxy.createDir(path);
            emptyDir=true;
        }
    }


    protected void errorWhenNotExist() throws IOException{
        if(!hadoopFileSystemProxy.fileExist(path)){

            throw new  SeaTunnelRuntimeException(SeaTunnelAPIErrorCode.SINK_TABLE_NOT_EXIST,
                    SeaTunnelAPIErrorCode.SINK_TABLE_NOT_EXIST.getDescription());
        }
    }

    protected void dropFile() throws IOException {
        FileStatus[] fileStatuses = hadoopFileSystemProxy.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            hadoopFileSystemProxy.deleteFile(fileStatus.getPath().toString());
        }
    }

    protected void errorWhenDataExists() throws IOException {
        try {
            if(hadoopFileSystemProxy.listStatus(path).length>0){
                throw new SeaTunnelRuntimeException(SeaTunnelAPIErrorCode.SOURCE_ALREADY_HAS_DATA,
                        SeaTunnelAPIErrorCode.SOURCE_ALREADY_HAS_DATA.getDescription());
            }
        }catch (FileNotFoundException ignore){}

    }
}
