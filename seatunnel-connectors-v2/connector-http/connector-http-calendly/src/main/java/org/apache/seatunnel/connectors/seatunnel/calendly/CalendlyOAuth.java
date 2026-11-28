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

package org.apache.seatunnel.connectors.seatunnel.calendly;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.connectors.seatunnel.calendly.CalendlyJson.Failure;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Rotates Calendly's single-use OAuth refresh token with a durable, exclusive store.
 *
 * <p>A persisted pending marker prevents replay after process death, a lost response, or an
 * uncertain disk write. The bearer token is used only after the replacement token is durable.
 */
final class CalendlyOAuth {
    private static final Set<PosixFilePermission> PRIVATE_FILE =
            PosixFilePermissions.fromString("rw-------");
    private static final int MAX_STORE_BYTES = 32 * 1024;

    private final CalendlyConfig config;
    private final CalendlyHttp http;
    private String currentAccessToken;
    private long refreshAfter;

    CalendlyOAuth(CalendlyConfig config, CalendlyHttp http) {
        this.config = config;
        this.http = http;
    }

    /**
     * Returns an access token, rotating and persisting its successor when necessary.
     *
     * @param deadline the deadline of the enclosing API read
     * @param force whether an authenticated data request already rejected the cached token
     * @return the cached or newly issued access token
     * @throws IOException if token rotation cannot be confirmed safely
     */
    String access(long deadline, boolean force) throws IOException {
        if (!force && currentAccessToken != null && refreshAfter - System.nanoTime() > 0) {
            return currentAccessToken;
        }
        currentAccessToken = null;
        Path configured = Path.of(config.tokenFile);
        try {
            Path directory = configured.getParent().toRealPath();
            PosixFileAttributes dir = attrs(directory);
            if (!dir.isDirectory()
                    || dir.permissions().contains(PosixFilePermission.GROUP_WRITE)
                    || dir.permissions().contains(PosixFilePermission.OTHERS_WRITE)) {
                throw CalendlyJson.failure(
                        "oauth_token_file requires a private writable directory");
            }
            Path store = directory.resolve(configured.getFileName());
            Path lockPath = store.resolveSibling(store.getFileName() + ".lock");
            try (FileChannel channel =
                            FileChannel.open(
                                    lockPath,
                                    Set.of(
                                            StandardOpenOption.CREATE,
                                            StandardOpenOption.WRITE,
                                            LinkOption.NOFOLLOW_LINKS),
                                    PosixFilePermissions.asFileAttribute(PRIVATE_FILE));
                    FileLock lock = channel.tryLock()) {
                if (lock == null) {
                    throw CalendlyJson.failure("OAuth token store is locked by another worker");
                }
                PosixFileAttributes lockFile = privateFile(lockPath);
                PosixFileAttributes stateFile = privateFile(store);
                if (!dir.owner().equals(stateFile.owner())
                        || !dir.owner().equals(lockFile.owner())) {
                    throw CalendlyJson.failure(
                            "OAuth token store, lock and directory must have the same owner");
                }
                return refresh(store, directory, deadline);
            }
        } catch (Failure known) {
            throw known;
        } catch (OverlappingFileLockException ignored) {
            throw CalendlyJson.failure("OAuth token store is locked by another worker");
        } catch (IOException | UnsupportedOperationException | SecurityException ignored) {
            throw CalendlyJson.failure("could not read, lock or durably update oauth_token_file");
        }
    }

    /**
     * Consumes one refresh token under the caller's file lock. Persisting the marker before the
     * POST closes the crash window between server-side consumption and local persistence.
     */
    private String refresh(Path store, Path directory, long deadline) throws IOException {
        String previous = readRefreshToken(store);
        String form =
                "grant_type=refresh_token&client_id="
                        + formValue(config.clientId)
                        + "&refresh_token="
                        + formValue(previous);
        String basic =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (config.clientId + ":" + config.clientSecret)
                                                .getBytes(StandardCharsets.UTF_8));
        http.checkActive(deadline);
        replace(store, directory, CalendlyJson.refreshDocument(null));
        long started = System.nanoTime();
        JsonNode data = http.rotate(basic, form.getBytes(StandardCharsets.UTF_8), deadline);
        String access = data.path("access_token").textValue();
        String next = data.path("refresh_token").textValue();
        JsonNode ttl = data.path("expires_in");
        if (!CalendlyConfig.validCredential(access)
                || !CalendlyConfig.validCredential(next)
                || previous.equals(next)
                || !"Bearer".equalsIgnoreCase(data.path("token_type").textValue())
                || !ttl.isNumber()
                || !ttl.canConvertToInt()
                || !ttl.canConvertToExactIntegral()
                || ttl.intValue() <= 0
                || ttl.intValue() > 86400) {
            throw CalendlyJson.failure("invalid OAuth rotation response; reauthorize Calendly");
        }
        replace(store, directory, CalendlyJson.refreshDocument(next));
        int lifetime = ttl.intValue();
        refreshAfter = started + TimeUnit.SECONDS.toNanos(lifetime - Math.min(30, lifetime / 10));
        currentAccessToken = access;
        return access;
    }

    private static String readRefreshToken(Path store) throws IOException {
        byte[] bytes;
        try (InputStream input = Files.newInputStream(store, LinkOption.NOFOLLOW_LINKS)) {
            bytes = input.readNBytes(MAX_STORE_BYTES + 1);
        }
        if (bytes.length > MAX_STORE_BYTES) {
            throw CalendlyJson.failure("oauth_token_file exceeds the store size limit");
        }
        JsonNode data;
        try {
            data = CalendlyJson.parse(bytes);
        } catch (Failure ignored) {
            throw CalendlyJson.failure("invalid oauth_token_file format");
        }
        JsonNode version = data.path("version");
        if (!version.isIntegralNumber()
                || !version.canConvertToInt()
                || version.intValue() != 1
                || data.size() != 2) {
            throw CalendlyJson.failure("invalid oauth_token_file format");
        }
        if (data.path("pending").isBoolean() && data.path("pending").booleanValue()) {
            throw CalendlyJson.failure("OAuth rotation outcome is unknown; reauthorize Calendly");
        }
        String token = data.path("refresh_token").textValue();
        if (!CalendlyConfig.validCredential(token)) {
            throw CalendlyJson.failure("oauth_token_file must contain a valid refresh_token");
        }
        return token;
    }

    /**
     * Replaces the whole state on the same filesystem and makes the directory entry durable.
     *
     * <p>Restrictive permissions are applied when creating the temporary file, before any token
     * bytes can be written.
     */
    private static void replace(Path target, Path directory, byte[] contents) throws IOException {
        Path temp =
                Files.createTempFile(
                        directory,
                        ".seatunnel-calendly-",
                        ".token",
                        PosixFilePermissions.asFileAttribute(PRIVATE_FILE));
        try {
            try (FileChannel file = FileChannel.open(temp, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(contents);
                while (buffer.hasRemaining()) {
                    file.write(buffer);
                }
                file.force(true);
            }
            Files.move(
                    temp,
                    target,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel dir = FileChannel.open(directory, StandardOpenOption.READ)) {
                dir.force(true);
            }
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    private static PosixFileAttributes privateFile(Path path) throws IOException {
        PosixFileAttributes info = attrs(path);
        if (!info.isRegularFile() || !info.permissions().equals(PRIVATE_FILE)) {
            throw CalendlyJson.failure(
                    "OAuth token store and lock must be regular mode 0600 files");
        }
        return info;
    }

    private static PosixFileAttributes attrs(Path path) throws IOException {
        return Files.readAttributes(path, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    }

    private static String formValue(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
