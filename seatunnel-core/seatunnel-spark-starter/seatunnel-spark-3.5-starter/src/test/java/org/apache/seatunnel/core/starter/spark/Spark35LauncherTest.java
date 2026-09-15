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

package org.apache.seatunnel.core.starter.spark;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class Spark35LauncherTest {

    @TempDir Path tempDir;

    private Path shellLauncher;
    private Path javaCapture;
    private Path sparkCapture;
    private Path sparkArguments;
    private ProcessBuilder process;

    private void prepareShellLauncher() throws IOException {
        Assumptions.assumeFalse(System.getProperty("os.name").startsWith("Windows"));
        Path home = Files.createDirectories(tempDir.resolve("seatunnel home"));
        Path bin = Files.createDirectories(home.resolve("bin"));
        Path config = Files.createDirectories(home.resolve("config"));
        Files.createFile(config.resolve("log4j2.properties"));
        shellLauncher = bin.resolve("start-seatunnel-spark-3.5-connector-v2.sh");
        Files.copy(
                locateWindowsLauncher().resolveSibling(shellLauncher.getFileName()), shellLauncher);
        Path sparkBin = Files.createDirectories(tempDir.resolve("spark home/bin"));
        Path mockBin = Files.createDirectories(tempDir.resolve("mock-bin"));
        javaCapture = tempDir.resolve("java-args");
        sparkCapture = tempDir.resolve("spark-args");
        sparkArguments = tempDir.resolve("expected-args");
        writeExecutable(
                mockBin.resolve("java"),
                "#!/bin/bash\n"
                        + "printf '%s\\0' \"$@\" > \"$JAVA_CAPTURE\"\n"
                        + "case $MODE in\n"
                        + "  help) printf 'usage line 1\\nusage line 2\\n'; exit 234;;\n"
                        + "  failure) exit 37;;\n"
                        + "  diagnostic) echo 'starter failed'; exit 38;;\n"
                        + "  empty) exit 0;;\n"
                        + "esac\n"
                        + "for arg in \"$@\"; do\n"
                        + "  case $arg in\n"
                        + "    -Dseatunnel.spark.starter.args-file=*)\n"
                        + "      cp \"$SPARK_ARGUMENTS\" \"${arg#*=}\"\n"
                        + "      echo 'starter diagnostic, not a command'; exit 0;;\n"
                        + "  esac\n"
                        + "done\n"
                        + "printf '%s\\n' \"$LEGACY_COMMAND\"\n");
        writeExecutable(
                sparkBin.resolve("spark-submit"),
                "#!/bin/bash\nprintf '%s\\0' \"$@\" > \"$SPARK_CAPTURE\"\nexit \"$SPARK_EXIT\"\n");
        process = new ProcessBuilder();
        process.environment().put("PATH", mockBin + File.pathSeparator + System.getenv("PATH"));
        process.environment().put("SPARK_HOME", sparkBin.getParent().toString());
        process.environment().put("JAVA_CAPTURE", javaCapture.toString());
        process.environment().put("SPARK_CAPTURE", sparkCapture.toString());
        process.environment().put("SPARK_ARGUMENTS", sparkArguments.toString());
        process.environment()
                .put("TMPDIR", Files.createDirectory(tempDir.resolve("tmp")).toString());
        process.environment().put("JAVA_OPTS", "-Xmx128m -Dliteral=*");
        process.environment().put("MODE", "success");
        process.environment().put("SPARK_EXIT", "0");
        process.environment().put("LEGACY_COMMAND", "unused");
        Files.write(sparkArguments, "--name\0job\0".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void preserveLiteralArgumentsWithoutEvaluatingConfig() throws Exception {
        prepareShellLauncher();
        Path marker = tempDir.resolve("executed");
        String value = "$(touch '" + marker + "') `id` $HOME * \"quote\" ! % & ;\nnext line";
        String arguments =
                "--name\0"
                        + value
                        + "\0--conf\0spark.app.name="
                        + value
                        + "\0-i\0table=order archive\0\0";
        Files.write(sparkArguments, arguments.getBytes(StandardCharsets.UTF_8));
        process.environment().put("LEGACY_COMMAND", "echo \"" + value + "\"");

        Assertions.assertEquals(
                0, runShell("--config", "job file.conf", "--name", "literal * job"));
        Assertions.assertArrayEquals(
                Files.readAllBytes(sparkArguments), Files.readAllBytes(sparkCapture));
        Assertions.assertFalse(Files.exists(marker));
        List<String> javaArgs = readArguments(javaCapture);
        Assertions.assertEquals("job file.conf", javaArgs.get(javaArgs.indexOf("--config") + 1));
        Assertions.assertEquals("literal * job", javaArgs.get(javaArgs.indexOf("--name") + 1));
        Assertions.assertTrue(javaArgs.contains("-Dliteral=*"));
        Assertions.assertTrue(
                javaArgs.get(javaArgs.indexOf("-cp") + 1)
                        .contains("seatunnel home/starter/logging/*:"));
        Assertions.assertTrue(
                javaArgs.stream()
                        .anyMatch(arg -> arg.endsWith("seatunnel home/config/log4j2.properties")));
    }

    @Test
    public void propagateSparkSubmitFailure() throws Exception {
        prepareShellLauncher();
        process.environment().put("SPARK_EXIT", "41");
        Assertions.assertEquals(41, runShell("--config", "job.conf"));
    }

    @Test
    public void preserveNoGlobConfiguredByEnvironment() throws Exception {
        prepareShellLauncher();
        Files.write(
                shellLauncher.getParent().getParent().resolve("config/seatunnel-env.sh"),
                ("set -f\njava() {\n"
                                + "  case $- in\n"
                                + "    *f*) command java \"$@\";;\n"
                                + "    *) return 92;;\n"
                                + "  esac\n}\n")
                        .getBytes(StandardCharsets.UTF_8));
        process.environment().put("JAVA_OPTS", "-Xmx128m\n-Dliteral=*");
        Assertions.assertEquals(0, runShell("--config", "job.conf"));
        Assertions.assertTrue(readArguments(javaCapture).contains("-Dliteral=*"));
    }

    @Test
    public void returnHelpWithoutSubmitting() throws Exception {
        prepareShellLauncher();
        process.environment().put("MODE", "help");
        Assertions.assertEquals(0, runShell());
        Assertions.assertEquals(
                "-h", readArguments(javaCapture).get(readArguments(javaCapture).size() - 1));
        Assertions.assertFalse(Files.exists(sparkCapture));
        Assertions.assertEquals(
                "usage line 1\nusage line 2\n",
                new String(Files.readAllBytes(tempDir.resolve("output")), StandardCharsets.UTF_8));
    }

    @Test
    public void propagateJavaFailureWithEmptyOutput() throws Exception {
        prepareShellLauncher();
        process.environment().put("MODE", "failure");
        Assertions.assertEquals(37, runShell("--config", "job.conf"));
        Assertions.assertFalse(Files.exists(sparkCapture));
    }

    @Test
    public void propagateJavaFailureWithDiagnosticOutput() throws Exception {
        prepareShellLauncher();
        process.environment().put("MODE", "diagnostic");
        Assertions.assertEquals(38, runShell("--config", "job.conf"));
        Assertions.assertFalse(Files.exists(sparkCapture));
        Assertions.assertTrue(
                new String(Files.readAllBytes(tempDir.resolve("output")), StandardCharsets.UTF_8)
                        .contains("starter failed"));
    }

    @Test
    public void rejectEmptySuccessfulOutput() throws Exception {
        prepareShellLauncher();
        process.environment().put("MODE", "empty");
        Assertions.assertEquals(1, runShell("--config", "job.conf"));
        Assertions.assertFalse(Files.exists(sparkCapture));
    }

    private int runShell(String... arguments) throws Exception {
        List<String> command = new ArrayList<>(Arrays.asList("bash", shellLauncher.toString()));
        command.addAll(Arrays.asList(arguments));
        Process child =
                process.command(command)
                        .redirectErrorStream(true)
                        .redirectOutput(tempDir.resolve("output").toFile())
                        .start();
        try {
            Assertions.assertTrue(child.waitFor(30, TimeUnit.SECONDS), "launcher did not exit");
            try (Stream<Path> remaining = Files.list(tempDir.resolve("tmp"))) {
                Assertions.assertEquals(
                        0, remaining.count(), "launcher did not clean up its argument file");
            }
            return child.exitValue();
        } finally {
            child.destroyForcibly();
        }
    }

    private List<String> readArguments(Path path) throws IOException {
        return Arrays.asList(
                new String(Files.readAllBytes(path), StandardCharsets.UTF_8).split("\0"));
    }

    private void writeExecutable(Path path, String content) throws IOException {
        Files.write(path, content.getBytes(StandardCharsets.UTF_8));
        Assertions.assertTrue(path.toFile().setExecutable(true));
    }

    @Test
    public void resolveWindowsApplicationDirectoryFromBinDirectory() throws IOException {
        String launcher =
                new String(Files.readAllBytes(locateWindowsLauncher()), StandardCharsets.UTF_8);

        Assertions.assertTrue(
                launcher.contains("for %%D in (\"%PRG_DIR%..\") do set \"APP_DIR=%%~fD\""));
        Assertions.assertFalse(launcher.contains("set \"APP_DIR=%~dp0\""));
        Assertions.assertTrue(launcher.contains("set \"CONF_DIR=%APP_DIR%\\config\""));
        Assertions.assertTrue(
                launcher.contains("set \"APP_JAR=%APP_DIR%\\starter\\%APP_JAR_NAME%\""));
    }

    @Test
    public void captureWindowsJavaStatusBeforeReadingOutput() throws IOException {
        String launcher =
                new String(Files.readAllBytes(locateWindowsLauncher()), StandardCharsets.UTF_8);
        Assertions.assertFalse(launcher.contains("!CMD!"));
        Assertions.assertFalse(launcher.contains("!errorlevel!"));
        Assertions.assertTrue(launcher.contains("java %JAVA_OPTS% -cp \"%CLASS_PATH%\""));
        int status = launcher.indexOf("set \"EXIT_CODE=%errorlevel%\"");
        int readOutput = launcher.indexOf("for /f");
        Assertions.assertTrue(status > 0 && status < readOutput);
        Assertions.assertTrue(launcher.contains("exit /b %EXIT_CODE%"));
        Assertions.assertTrue(
                launcher.contains("call \"%SPARK_HOME%\\bin\\spark-submit.cmd\" %CMD%"));
    }

    private Path locateWindowsLauncher() {
        Path modulePath = Paths.get("src/main/bin/start-seatunnel-spark-3.5-connector-v2.cmd");
        if (Files.exists(modulePath)) {
            return modulePath;
        }
        return Paths.get(
                "seatunnel-core/seatunnel-spark-starter/seatunnel-spark-3.5-starter/"
                        + "src/main/bin/start-seatunnel-spark-3.5-connector-v2.cmd");
    }
}
