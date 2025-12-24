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

package org.apache.seatunnel.format.protobuf;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.format.protobuf.exception.ProtobufFormatErrorCode;
import org.apache.seatunnel.format.protobuf.exception.SeaTunnelProtobufFormatException;

import com.github.os72.protocjar.Protoc;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class CompileDescriptor {

    public static Descriptors.Descriptor compileDescriptorTempFile(
            String protoContent, String messageName)
            throws IOException, InterruptedException, Descriptors.DescriptorValidationException {
        // Because Protobuf can only be dynamically parsed through the descriptor file, the file
        // needs to be compiled and generated. The following method is used here to solve the
        // problem: generate a temporary directory and compile .proto into a descriptor temporary
        // file. The temporary file and directory are deleted after the JVM runs.
        File tmpDir = createTempDirectory();
        File protoFile = createProtoFile(tmpDir, protoContent);
        String targetDescPath = compileProtoToDescriptor(tmpDir, protoFile);

        try (FileInputStream fis = new FileInputStream(targetDescPath)) {
            DescriptorProtos.FileDescriptorSet descriptorSet =
                    DescriptorProtos.FileDescriptorSet.parseFrom(fis);
            Descriptors.FileDescriptor[] descriptorsArray = buildFileDescriptors(descriptorSet);
            return descriptorsArray[0].findMessageTypeByName(messageName);
        } finally {
            tmpDir.delete();
            protoFile.delete();
            new File(targetDescPath).delete();
        }
    }

    private static File createTempDirectory() throws IOException {
        File tmpDir = File.createTempFile("tmp_protobuf_", "_proto");
        tmpDir.delete();
        tmpDir.mkdirs();
        tmpDir.deleteOnExit();
        return tmpDir;
    }

    private static File createProtoFile(File tmpDir, String protoContent) throws IOException {
        File protoFile = new File(tmpDir, ".proto");
        protoFile.deleteOnExit();
        FileUtils.writeStringToFile(protoFile.getPath(), protoContent);
        return protoFile;
    }

    private static String compileProtoToDescriptor(File tmpDir, File protoFile)
            throws IOException, InterruptedException {
        String targetDesc = tmpDir + "/.desc";
        new File(targetDesc).deleteOnExit();

        int exitCode =
                Protoc.runProtoc(
                        new String[] {
                            "--proto_path=" + protoFile.getParent(),
                            "--descriptor_set_out=" + targetDesc,
                            protoFile.getPath()
                        });

        if (exitCode != 0) {
            throw new SeaTunnelProtobufFormatException(
                    ProtobufFormatErrorCode.DESCRIPTOR_CONVERT_FAILED,
                    "Protoc compile error, exit code: " + exitCode);
        }
        return targetDesc;
    }

    private static Descriptors.FileDescriptor[] buildFileDescriptors(
            DescriptorProtos.FileDescriptorSet descriptorSet)
            throws Descriptors.DescriptorValidationException {
        List<DescriptorProtos.FileDescriptorProto> fileDescriptors = descriptorSet.getFileList();
        Descriptors.FileDescriptor[] descriptorsArray =
                new Descriptors.FileDescriptor[fileDescriptors.size()];
        for (int i = 0; i < fileDescriptors.size(); i++) {
            descriptorsArray[i] =
                    Descriptors.FileDescriptor.buildFrom(
                            fileDescriptors.get(i), new Descriptors.FileDescriptor[] {});
        }
        return descriptorsArray;
    }
}
