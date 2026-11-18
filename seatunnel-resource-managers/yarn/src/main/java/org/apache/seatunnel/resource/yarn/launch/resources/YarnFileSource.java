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

package org.apache.seatunnel.resource.yarn.launch.resources;

import org.apache.seatunnel.shade.com.google.common.io.Closer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/** A submitter-provided file that YARN can localize from the application's private staging area. */
@FunctionalInterface
public interface YarnFileSource {
    /**
     * Creates the target file in the application's staging directory during submission. The source
     * owns the streams it opens. The caller owns the destination filesystem.
     *
     * @param destination filesystem containing the private staging directory
     * @param target path reserved for this resource
     * @param configuration submitting user's Hadoop configuration for reading a source URI
     * @throws IOException if reading the source or creating the target fails
     */
    void stage(FileSystem destination, Path target, Configuration configuration) throws IOException;

    /**
     * Resolves unqualified paths on the submission host, even when its default Hadoop filesystem is
     * HDFS.
     *
     * @param location path or URI supplied in a deployment option
     * @param option option name used in validation errors
     * @return a local file URI or an absolute HDFS URI
     * @throws IllegalArgumentException if the location has an unsupported URI form
     */
    static URI sourceUri(String location, String option) {
        File file = new File(location);
        if (file.isAbsolute() || new Path(location).toUri().getScheme() == null) {
            return file.toURI();
        }
        URI uri = URI.create(location);
        if (uri.isOpaque()
                || uri.getPath() == null
                || !uri.getPath().startsWith("/")
                || uri.getQuery() != null
                || uri.getFragment() != null
                || !("hdfs".equals(uri.getScheme()) || "file".equals(uri.getScheme()))) {
            throw new IllegalArgumentException(
                    option + " must be a local path, file URI or absolute hdfs URI: " + location);
        }
        return uri;
    }

    /** Streams a local or HDFS file using the submitter's Hadoop identity. */
    static YarnFileSource fromUri(URI source) {
        return (destination, target, configuration) -> {
            Closer closer = Closer.create();
            try {
                FileSystem sourceFs =
                        closer.register(FileSystem.newInstance(source, configuration));
                InputStream input = closer.register(sourceFs.open(new Path(source)));
                OutputStream output = closer.register(destination.create(target, false));
                input.transferTo(output);
            } catch (Throwable failure) {
                throw closer.rethrow(failure);
            } finally {
                closer.close();
            }
        };
    }

    /** Writes inline text verbatim, including whitespace and any final newline. */
    static YarnFileSource inline(String contents) {
        return (destination, target, configuration) -> {
            Closer closer = Closer.create();
            try {
                Writer output =
                        closer.register(
                                new OutputStreamWriter(
                                        destination.create(target, false), StandardCharsets.UTF_8));
                output.write(contents);
            } catch (Throwable failure) {
                throw closer.rethrow(failure);
            } finally {
                closer.close();
            }
        };
    }
}
