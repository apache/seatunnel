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

package org.apache.seatunnel.installer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Verifies the connector installer download, integrity, cleanup, and Maven compatibility paths
 * without relying on an external artifact repository.
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
public class InstallPluginScriptTest {

    private static final byte[] JAR_CONTENT =
            new byte[] {0x50, 0x4B, 0x03, 0x04, 'f', 'i', 'x', 't', 'u', 'r', 'e'};

    @TempDir private Path temporaryDirectory;

    /**
     * Verifies that a release artifact is installed only after its checksum and JAR header pass.
     */
    @Test
    public void testReleaseDownloadWithChecksum() throws Exception {
        Path distribution = createDistribution("connector-fake");
        Path fakeBinaryDirectory = createFakeCurl(sha1Hex(JAR_CONTENT), false, 404);

        int exitCode = runInstaller(distribution, fakeBinaryDirectory, "2.3.13", null);

        Assertions.assertEquals(0, exitCode);
        Path installedJar = distribution.resolve("connectors").resolve("connector-fake-2.3.13.jar");
        Assertions.assertArrayEquals(JAR_CONTENT, Files.readAllBytes(installedJar));
        Assertions.assertEquals(
                EnumSet.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.GROUP_READ,
                        PosixFilePermission.OTHERS_READ),
                Files.getPosixFilePermissions(installedJar));
        assertNoTemporaryFiles(distribution.resolve("connectors"));
    }

    /**
     * Verifies that an environment with only SHA-1 support requests the SHA-1 sidecar directly,
     * even when the repository would publish SHA-512.
     */
    @Test
    public void testSha1OnlyEnvironmentUsesSha1Checksum() throws Exception {
        Path distribution = createDistribution("connector-fake");
        Path fakeBinaryDirectory = createFakeCurl(sha1Hex(JAR_CONTENT), true, 200);
        createIsolatedSha1Tools(fakeBinaryDirectory, sha1Hex(JAR_CONTENT));

        int exitCode = runInstaller(distribution, fakeBinaryDirectory, "2.3.13", null, true);

        Assertions.assertEquals(0, exitCode);
        Path installedJar = distribution.resolve("connectors").resolve("connector-fake-2.3.13.jar");
        Assertions.assertArrayEquals(JAR_CONTENT, Files.readAllBytes(installedJar));
        assertNoTemporaryFiles(distribution.resolve("connectors"));
    }

    /**
     * Verifies that the preferred SHA-512 checksum path installs a valid release artifact without
     * falling back to SHA-1.
     */
    @Test
    public void testReleaseDownloadWithSha512Checksum() throws Exception {
        Path distribution = createDistribution("connector-fake");
        Path fakeBinaryDirectory = createFakeCurl(sha512Hex(JAR_CONTENT), true, 200);

        int exitCode = runInstaller(distribution, fakeBinaryDirectory, "2.3.13", null);

        Assertions.assertEquals(0, exitCode);
        Path installedJar = distribution.resolve("connectors").resolve("connector-fake-2.3.13.jar");
        Assertions.assertArrayEquals(JAR_CONTENT, Files.readAllBytes(installedJar));
        assertNoTemporaryFiles(distribution.resolve("connectors"));
    }

    /**
     * Verifies that a checksum mismatch leaves an existing connector untouched and cleans staging
     * files.
     */
    @Test
    public void testChecksumFailurePreservesExistingConnector() throws Exception {
        Path distribution = createDistribution("connector-fake");
        Path connectorsDirectory = distribution.resolve("connectors");
        Files.createDirectories(connectorsDirectory);
        Path installedJar = connectorsDirectory.resolve("connector-fake-2.3.13.jar");
        byte[] existingContent = "existing-connector".getBytes(StandardCharsets.UTF_8);
        Files.write(installedJar, existingContent);
        Path fakeBinaryDirectory = createFakeCurl(repeat("0", 40), false, 404);

        int exitCode = runInstaller(distribution, fakeBinaryDirectory, "2.3.13", null);

        Assertions.assertNotEquals(0, exitCode);
        Assertions.assertArrayEquals(existingContent, Files.readAllBytes(installedJar));
        assertNoTemporaryFiles(connectorsDirectory);
    }

    /**
     * Verifies that a SHA-512 server error fails closed instead of silently falling back to SHA-1.
     */
    @Test
    public void testSha512ServerFailureDoesNotFallbackToSha1() throws Exception {
        Path distribution = createDistribution("connector-fake");
        Path fakeBinaryDirectory = createFakeCurl(sha1Hex(JAR_CONTENT), false, 500);

        int exitCode = runInstaller(distribution, fakeBinaryDirectory, "2.3.13", null);

        Assertions.assertNotEquals(0, exitCode);
        Assertions.assertFalse(
                Files.exists(
                        distribution.resolve("connectors").resolve("connector-fake-2.3.13.jar")));
        assertNoTemporaryFiles(distribution.resolve("connectors"));
    }

    /**
     * Verifies that an HTTP failure cannot replace an existing connector and leaves no staging
     * files behind.
     */
    @Test
    public void testDownloadFailurePreservesExistingConnector() throws Exception {
        Path distribution = createDistribution("connector-fake");
        Path connectorsDirectory = distribution.resolve("connectors");
        Files.createDirectories(connectorsDirectory);
        Path installedJar = connectorsDirectory.resolve("connector-fake-2.3.13.jar");
        byte[] existingContent = "existing-connector".getBytes(StandardCharsets.UTF_8);
        Files.write(installedJar, existingContent);
        Path fakeBinaryDirectory = createFailingCurl();

        int exitCode = runInstaller(distribution, fakeBinaryDirectory, "2.3.13", null);

        Assertions.assertNotEquals(0, exitCode);
        Assertions.assertArrayEquals(existingContent, Files.readAllBytes(installedJar));
        assertNoTemporaryFiles(connectorsDirectory);
    }

    /**
     * Verifies that a checksum-matching response is still rejected when it does not have a JAR
     * archive header.
     */
    @Test
    public void testNonJarPayloadIsRejected() throws Exception {
        byte[] nonJarContent = "not-a-jar".getBytes(StandardCharsets.UTF_8);
        Path distribution = createDistribution("connector-fake");
        Path fakeBinaryDirectory =
                createFakeCurl(sha1Hex(nonJarContent), false, 404, "printf 'not-a-jar'");

        int exitCode = runInstaller(distribution, fakeBinaryDirectory, "2.3.13", null);

        Assertions.assertNotEquals(0, exitCode);
        Assertions.assertFalse(
                Files.exists(
                        distribution.resolve("connectors").resolve("connector-fake-2.3.13.jar")));
        assertNoTemporaryFiles(distribution.resolve("connectors"));
    }

    /**
     * Verifies that snapshot versions retain Maven metadata resolution instead of using a direct
     * release URL.
     */
    @Test
    public void testSnapshotUsesMavenFallback() throws Exception {
        Path distribution = createDistribution("connector-fake");
        Path fakeBinaryDirectory = createFakeCurl(repeat("0", 40), false, 404);
        Path capturedArguments = temporaryDirectory.resolve("maven-arguments.txt");
        createFakeMavenWrapper(distribution, capturedArguments);

        int exitCode =
                runInstaller(
                        distribution, fakeBinaryDirectory, "3.0.0-SNAPSHOT", capturedArguments);

        Assertions.assertEquals(0, exitCode);
        String arguments =
                new String(Files.readAllBytes(capturedArguments), StandardCharsets.UTF_8);
        Assertions.assertTrue(arguments.contains("-Dversion=3.0.0-SNAPSHOT"));
        Assertions.assertTrue(arguments.contains("-DartifactId=connector-fake"));
    }

    /**
     * Verifies that a fixed release can explicitly retain Maven resolution behavior for existing
     * mirror, proxy, and authentication policies.
     */
    @Test
    public void testReleaseUsesExplicitMavenFallback() throws Exception {
        Path distribution = createDistribution("connector-fake");
        Path fakeBinaryDirectory = createFakeCurl(repeat("0", 40), false, 404);
        Path capturedArguments = temporaryDirectory.resolve("release-maven-arguments.txt");
        createFakeMavenWrapper(distribution, capturedArguments);

        int exitCode =
                runInstaller(
                        distribution,
                        fakeBinaryDirectory,
                        "2.3.13",
                        capturedArguments,
                        false,
                        "maven",
                        "https://repo.example.test/maven2");

        Assertions.assertEquals(0, exitCode);
        String arguments =
                new String(Files.readAllBytes(capturedArguments), StandardCharsets.UTF_8);
        Assertions.assertTrue(arguments.contains("-Dversion=2.3.13"));
        Assertions.assertTrue(arguments.contains("-DartifactId=connector-fake"));
    }

    /**
     * Verifies that unsafe release version text is rejected before a download is attempted or a
     * connectors directory is created.
     */
    @Test
    public void testInvalidReleaseVersionIsRejected() throws Exception {
        Path distribution = createDistribution("connector-fake");
        Path fakeBinaryDirectory = createFakeCurl(sha1Hex(JAR_CONTENT), false, 404);

        int exitCode = runInstaller(distribution, fakeBinaryDirectory, "../2.3.13", null);

        Assertions.assertNotEquals(0, exitCode);
        Assertions.assertFalse(Files.exists(distribution.resolve("connectors")));
    }

    /**
     * Verifies that an unsafe connector artifact ID is rejected before any artifact download is
     * attempted.
     */
    @Test
    public void testInvalidArtifactIdIsRejected() throws Exception {
        Path distribution = createDistribution("../connector-fake");
        Path fakeBinaryDirectory = createFakeCurl(sha1Hex(JAR_CONTENT), false, 404);

        int exitCode = runInstaller(distribution, fakeBinaryDirectory, "2.3.13", null);

        Assertions.assertNotEquals(0, exitCode);
        assertNoTemporaryFiles(distribution.resolve("connectors"));
    }

    /**
     * Verifies that the direct-download repository cannot use plaintext HTTP as its base protocol.
     */
    @Test
    public void testHttpRepositoryIsRejected() throws Exception {
        Path distribution = createDistribution("connector-fake");
        Path fakeBinaryDirectory = createFakeCurl(sha1Hex(JAR_CONTENT), false, 404);

        int exitCode =
                runInstaller(
                        distribution,
                        fakeBinaryDirectory,
                        "2.3.13",
                        null,
                        false,
                        null,
                        "http://repo.example.test/maven2");

        Assertions.assertNotEquals(0, exitCode);
        Assertions.assertFalse(Files.exists(distribution.resolve("connectors")));
    }

    /**
     * Creates a minimal SeaTunnel distribution containing the installer and selected plugin
     * configuration.
     *
     * @param pluginConfig plugin artifact ID written to the installer configuration
     * @return path to the minimal distribution
     */
    private Path createDistribution(String pluginConfig) throws Exception {
        Path distribution = temporaryDirectory.resolve("SeaTunnel home with spaces");
        Path binDirectory = distribution.resolve("bin");
        Path configDirectory = distribution.resolve("config");
        Files.createDirectories(binDirectory);
        Files.createDirectories(configDirectory);
        Files.copy(locateFile("bin/install-plugin.sh"), binDirectory.resolve("install-plugin.sh"));
        Assertions.assertTrue(
                binDirectory.resolve("install-plugin.sh").toFile().setExecutable(true));
        Files.write(
                configDirectory.resolve("plugin_config"),
                (pluginConfig + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
        return distribution;
    }

    /**
     * Creates a deterministic curl replacement that serves a JAR fixture and the requested checksum
     * responses.
     *
     * @param checksum checksum response returned by the fake repository
     * @param sha512Available whether the fake repository publishes SHA-512
     * @param sha512Status HTTP status returned for the SHA-512 request
     * @return directory containing the fake curl executable
     */
    private Path createFakeCurl(String checksum, boolean sha512Available, int sha512Status)
            throws Exception {
        return createFakeCurl(
                checksum, sha512Available, sha512Status, "printf '\\120\\113\\003\\004fixture'");
    }

    /**
     * Creates a deterministic curl replacement with a configurable artifact response.
     *
     * @param checksum checksum response returned by the fake repository
     * @param sha512Available whether the fake repository publishes SHA-512
     * @param sha512Status HTTP status returned for the SHA-512 request
     * @param artifactCommand shell command that writes artifact bytes to standard output
     * @return directory containing the fake curl executable
     */
    private Path createFakeCurl(
            String checksum, boolean sha512Available, int sha512Status, String artifactCommand)
            throws Exception {
        Path fakeBinaryDirectory =
                temporaryDirectory.resolve("fake-bin-" + checksum.substring(0, 8));
        Files.createDirectories(fakeBinaryDirectory);
        Path fakeCurl = fakeBinaryDirectory.resolve("curl");
        String script =
                "#!/bin/sh\n"
                        + "output=\n"
                        + "url=\n"
                        + "secure_proto=false\n"
                        + "secure_redirect=false\n"
                        + "while [ \"$#\" -gt 0 ]; do\n"
                        + "  case \"$1\" in\n"
                        + "    --output) output=$2; shift 2 ;;\n"
                        + "    --proto) [ \"$2\" = '=https' ] && secure_proto=true; shift 2 ;;\n"
                        + "    --proto-redir) [ \"$2\" = '=https' ] && secure_redirect=true;"
                        + " shift 2 ;;\n"
                        + "    --retry|--connect-timeout|--write-out)"
                        + " shift 2 ;;\n"
                        + "    --fail|--location) shift ;;\n"
                        + "    *) url=$1; shift ;;\n"
                        + "  esac\n"
                        + "done\n"
                        + "[ \"$secure_proto\" = true ] || exit 90\n"
                        + "[ \"$secure_redirect\" = true ] || exit 91\n"
                        + "case \"$url\" in\n"
                        + "  *.jar.sha512)\n"
                        + (sha512Available
                                ? "    printf '%s\\n' '" + checksum + "' > \"$output\"\n"
                                : "    printf '%s' '" + sha512Status + "'; exit 22\n")
                        + "    ;;\n"
                        + "  *.jar.sha1) printf '%s\\n' '"
                        + checksum
                        + "' > \"$output\" ;;\n"
                        + "  *.jar) "
                        + artifactCommand
                        + " > \"$output\" ;;\n"
                        + "  *) exit 22 ;;\n"
                        + "esac\n"
                        + "printf '%s' '200'\n";
        Files.write(fakeCurl, script.getBytes(StandardCharsets.UTF_8));
        Assertions.assertTrue(fakeCurl.toFile().setExecutable(true));
        return fakeBinaryDirectory;
    }

    /**
     * Creates a curl replacement that simulates an HTTP failure for every request.
     *
     * @return directory containing the failing curl executable
     */
    private Path createFailingCurl() throws Exception {
        Path fakeBinaryDirectory = temporaryDirectory.resolve("failing-curl-bin");
        Files.createDirectories(fakeBinaryDirectory);
        Path fakeCurl = fakeBinaryDirectory.resolve("curl");
        Files.write(fakeCurl, "#!/bin/sh\nexit 22\n".getBytes(StandardCharsets.UTF_8));
        Assertions.assertTrue(fakeCurl.toFile().setExecutable(true));
        return fakeBinaryDirectory;
    }

    /**
     * Adds only a SHA-1 implementation and the POSIX utilities needed by the installer to an
     * isolated PATH.
     *
     * @param fakeBinaryDirectory isolated executable directory
     * @param checksum checksum returned by the fake SHA-1 implementation
     */
    private void createIsolatedSha1Tools(Path fakeBinaryDirectory, String checksum)
            throws Exception {
        for (String tool :
                new String[] {
                    "awk", "cut", "dirname", "mkdir", "mktemp", "mv", "od", "rm", "rmdir", "tr"
                }) {
            Path source = locateSystemTool(tool);
            Files.createSymbolicLink(fakeBinaryDirectory.resolve(tool), source);
        }

        Path fakeSha1sum = fakeBinaryDirectory.resolve("sha1sum");
        String script = "#!/bin/sh\n" + "printf '%s  %s\\n' '" + checksum + "' \"$1\"\n";
        Files.write(fakeSha1sum, script.getBytes(StandardCharsets.UTF_8));
        Assertions.assertTrue(fakeSha1sum.toFile().setExecutable(true));
    }

    /**
     * Locates a standard POSIX utility for an isolated test PATH.
     *
     * @param tool executable name
     * @return absolute executable path
     */
    private Path locateSystemTool(String tool) {
        for (String directory : new String[] {"/usr/bin", "/bin"}) {
            Path candidate = Paths.get(directory, tool);
            if (Files.isExecutable(candidate)) {
                return candidate;
            }
        }
        throw new IllegalStateException("Required POSIX test utility is unavailable: " + tool);
    }

    /**
     * Creates a Maven wrapper replacement that records dependency resolution arguments.
     *
     * @param distribution minimal SeaTunnel distribution
     * @param capturedArguments file that receives Maven arguments
     */
    private void createFakeMavenWrapper(Path distribution, Path capturedArguments)
            throws Exception {
        Path fakeMavenWrapper = distribution.resolve("mvnw");
        String script = "#!/bin/sh\n" + "printf '%s\\n' \"$@\" > \"" + capturedArguments + "\"\n";
        Files.write(fakeMavenWrapper, script.getBytes(StandardCharsets.UTF_8));
        Assertions.assertTrue(fakeMavenWrapper.toFile().setExecutable(true));
    }

    /**
     * Runs the installer with the fake download tools and a fixed HTTPS repository.
     *
     * @param distribution minimal SeaTunnel distribution
     * @param fakeBinaryDirectory directory prepended to PATH
     * @param version connector version passed to the installer
     * @param capturedArguments optional Maven argument capture file
     * @return installer process exit code
     */
    private int runInstaller(
            Path distribution, Path fakeBinaryDirectory, String version, Path capturedArguments)
            throws Exception {
        return runInstaller(distribution, fakeBinaryDirectory, version, capturedArguments, false);
    }

    /**
     * Runs the installer with optional isolation from host checksum executables.
     *
     * @param distribution minimal SeaTunnel distribution
     * @param fakeBinaryDirectory directory used as PATH
     * @param version connector version passed to the installer
     * @param capturedArguments optional Maven argument capture file
     * @param isolatePath whether PATH should exclude host checksum executables
     * @return installer process exit code
     */
    private int runInstaller(
            Path distribution,
            Path fakeBinaryDirectory,
            String version,
            Path capturedArguments,
            boolean isolatePath)
            throws Exception {
        return runInstaller(
                distribution,
                fakeBinaryDirectory,
                version,
                capturedArguments,
                isolatePath,
                null,
                "https://repo.example.test/maven2");
    }

    /**
     * Runs the installer with explicit method and repository environment values.
     *
     * @param distribution minimal SeaTunnel distribution
     * @param fakeBinaryDirectory directory used as PATH
     * @param version connector version passed to the installer
     * @param capturedArguments optional Maven argument capture file
     * @param isolatePath whether PATH should exclude host checksum executables
     * @param downloadMethod optional installer download method
     * @param mavenRepository repository base URL
     * @return installer process exit code
     */
    private int runInstaller(
            Path distribution,
            Path fakeBinaryDirectory,
            String version,
            Path capturedArguments,
            boolean isolatePath,
            String downloadMethod,
            String mavenRepository)
            throws Exception {
        Path output = temporaryDirectory.resolve("installer-output-" + version.hashCode() + ".txt");
        ProcessBuilder processBuilder =
                new ProcessBuilder(
                        "/bin/sh",
                        "-c",
                        "umask 022; exec /bin/sh \"$1\" \"$2\"",
                        "install-plugin-test",
                        distribution.resolve("bin").resolve("install-plugin.sh").toString(),
                        version);
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(output.toFile());
        Map<String, String> environment = processBuilder.environment();
        String path = fakeBinaryDirectory.toString();
        if (!isolatePath) {
            path += System.getProperty("path.separator") + environment.getOrDefault("PATH", "");
        }
        environment.put("PATH", path);
        environment.put("SEATUNNEL_MAVEN_REPOSITORY", mavenRepository);
        if (downloadMethod == null) {
            environment.remove("SEATUNNEL_PLUGIN_DOWNLOAD_METHOD");
        } else {
            environment.put("SEATUNNEL_PLUGIN_DOWNLOAD_METHOD", downloadMethod);
        }
        if (capturedArguments != null) {
            environment.put("CAPTURE_FILE", capturedArguments.toString());
        }
        Process process = processBuilder.start();
        return process.waitFor();
    }

    /**
     * Confirms that failed or completed installs leave no uniquely named staging files.
     *
     * @param connectorsDirectory connector installation directory
     */
    private void assertNoTemporaryFiles(Path connectorsDirectory) throws Exception {
        try (Stream<Path> files = Files.list(connectorsDirectory)) {
            Assertions.assertFalse(
                    files.anyMatch(
                            path -> path.getFileName().toString().contains(".install-plugin.")));
        }
    }

    /**
     * Resolves a repository file when tests run from either the root project or seatunnel-dist.
     *
     * @param relativePath repository-relative file path
     * @return resolved file path
     */
    private Path locateFile(String relativePath) {
        Path rootPath = Paths.get(relativePath);
        if (Files.exists(rootPath)) {
            return rootPath;
        }
        return Paths.get("..").resolve(relativePath);
    }

    /**
     * Calculates the lowercase SHA-1 digest used by the fake Maven repository.
     *
     * @param content bytes to hash
     * @return lowercase hexadecimal digest
     */
    private String sha1Hex(byte[] content) throws Exception {
        return digestHex("SHA-1", content);
    }

    /**
     * Calculates the lowercase SHA-512 digest used by the fake Maven repository.
     *
     * @param content bytes to hash
     * @return lowercase hexadecimal digest
     */
    private String sha512Hex(byte[] content) throws Exception {
        return digestHex("SHA-512", content);
    }

    /**
     * Calculates a lowercase hexadecimal digest.
     *
     * @param algorithm message digest algorithm
     * @param content bytes to hash
     * @return lowercase hexadecimal digest
     */
    private String digestHex(String algorithm, byte[] content) throws Exception {
        byte[] digest = MessageDigest.getInstance(algorithm).digest(content);
        StringBuilder value = new StringBuilder(digest.length * 2);
        for (byte element : digest) {
            value.append(String.format("%02x", element & 0xff));
        }
        return value.toString();
    }

    /**
     * Repeats a string to construct fixed-length invalid checksum fixtures.
     *
     * @param value value to repeat
     * @param count number of repetitions
     * @return repeated value
     */
    private String repeat(String value, int count) {
        StringBuilder result = new StringBuilder(value.length() * count);
        for (int index = 0; index < count; index++) {
            result.append(value);
        }
        return result.toString();
    }
}
