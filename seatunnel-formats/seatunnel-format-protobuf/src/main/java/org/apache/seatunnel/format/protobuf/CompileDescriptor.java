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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class CompileDescriptor {

    // The bundled protoc has no macOS ARM64 binary. Keep other platforms on their existing
    // compiler so working offline installations do not require a new download.
    private static final String MACOS_ARM64_PROTOC_VERSION = "3.25.3";

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

        List<String> protocArgs = new ArrayList<>();
        if (isMacOsArm64()) {
            // protoc-jar consumes "-v<version>" itself and resolves that exact compiler release
            // instead of its bundled one, which has no macOS ARM64 binary.
            protocArgs.add("-v" + MACOS_ARM64_PROTOC_VERSION);
        }
        protocArgs.add("--proto_path=" + protoFile.getParent());
        protocArgs.add("--descriptor_set_out=" + targetDesc);
        protocArgs.add(protoFile.getPath());

        int exitCode = Protoc.runProtoc(protocArgs.toArray(new String[0]));

        if (exitCode != 0) {
            throw new SeaTunnelProtobufFormatException(
                    ProtobufFormatErrorCode.DESCRIPTOR_CONVERT_FAILED,
                    "Protoc compile error, exit code: " + exitCode);
        }
        return targetDesc;
    }

    /** macOS JVMs may report either arm64 or aarch64 for Apple Silicon. */
    private static boolean isMacOsArm64() {
        String osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        boolean macOs = osName.startsWith("mac os x") || osName.startsWith("macosx");
        if (!macOs) {
            return false;
        }
        String osArch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        return "arm64".equals(osArch) || "aarch64".equals(osArch);
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
