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

package org.apache.seatunnel.connectors.seatunnel.iceberg.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.util.SerializableSupplier;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * A {@link FileIO} that keeps Parquet away from Hadoop APIs the runtime does not have.
 *
 * <p>For a Hadoop input file or stream, Iceberg hands the Hadoop objects to Parquet, and Parquet
 * 1.16 (required by Iceberg 1.10) then calls {@code FileSystem.openFile()}, added in Hadoop 3.3,
 * and {@code FSDataInputStream.hasCapability()}, added in Hadoop 3. SeaTunnel Zeta ships Hadoop 3.1
 * and Flink distributions may ship Hadoop 2, so reading Parquet data or delete files would fail
 * with {@link NoSuchMethodError}. On such runtimes this FileIO returns Hadoop input files whose
 * type and stream are hidden, so Parquet reads them through Iceberg's stream adapter, as it does
 * for object store FileIOs. Other input files, and all files on Hadoop 3.3 or later, are returned
 * unchanged. The legacy path cannot use Hadoop range reads; this is limited to runtimes that cannot
 * use Parquet's newer Hadoop path at all.
 *
 * <p>The no-argument constructor wraps a {@link HadoopFileIO}, so the class can also be set as a
 * catalog's {@code io-impl}.
 */
public class HadoopStreamFileIO implements FileIO, HadoopConfigurable {

    private static final long serialVersionUID = 1L;

    private static final boolean HADOOP_OPEN_FILE_AVAILABLE = hadoopOpenFileAvailable();

    private final FileIO delegate;

    public HadoopStreamFileIO() {
        this(new HadoopFileIO());
    }

    public HadoopStreamFileIO(FileIO delegate) {
        this.delegate = delegate;
    }

    @Override
    public InputFile newInputFile(String path) {
        return wrap(delegate.newInputFile(path));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return wrap(delegate.newInputFile(path, length));
    }

    @Override
    public InputFile newInputFile(DataFile file) {
        return wrap(delegate.newInputFile(file));
    }

    @Override
    public InputFile newInputFile(DeleteFile file) {
        return wrap(delegate.newInputFile(file));
    }

    @Override
    public InputFile newInputFile(ManifestFile manifest) {
        return wrap(delegate.newInputFile(manifest));
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
        delegate.deleteFile(path);
    }

    @Override
    public Map<String, String> properties() {
        return delegate.properties();
    }

    @Override
    public void initialize(Map<String, String> properties) {
        delegate.initialize(properties);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void setConf(Configuration conf) {
        if (delegate instanceof HadoopConfigurable) {
            ((HadoopConfigurable) delegate).setConf(conf);
        }
    }

    @Override
    public Configuration getConf() {
        return delegate instanceof HadoopConfigurable
                ? ((HadoopConfigurable) delegate).getConf()
                : null;
    }

    @Override
    public void serializeConfWith(
            Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
        if (delegate instanceof HadoopConfigurable) {
            ((HadoopConfigurable) delegate).serializeConfWith(confSerializer);
        }
    }

    /**
     * Returns the input file with its Hadoop type and stream hidden when the Hadoop runtime has no
     * {@code FileSystem.openFile()}, otherwise the input file itself.
     */
    public static InputFile wrap(InputFile file) {
        return wrap(file, HADOOP_OPEN_FILE_AVAILABLE);
    }

    static InputFile wrap(InputFile file, boolean hadoopOpenFileAvailable) {
        if (!hadoopOpenFileAvailable && file instanceof HadoopInputFile) {
            return new StreamInputFile(file);
        }
        return file;
    }

    private static boolean hadoopOpenFileAvailable() {
        try {
            FileSystem.class.getMethod("openFile", Path.class);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static class StreamInputFile implements InputFile {

        private final InputFile delegate;

        private StreamInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getLength() {
            return delegate.getLength();
        }

        @Override
        public SeekableInputStream newStream() {
            return new ForwardingInputStream(delegate.newStream());
        }

        @Override
        public String location() {
            return delegate.location();
        }

        @Override
        public boolean exists() {
            return delegate.exists();
        }
    }

    /** Deliberately not a {@code DelegatingInputStream}, so Parquet cannot unwrap it. */
    private static class ForwardingInputStream extends SeekableInputStream {

        private final SeekableInputStream delegate;

        private ForwardingInputStream(SeekableInputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
            delegate.seek(newPos);
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return delegate.read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return delegate.skip(n);
        }

        @Override
        public int available() throws IOException {
            return delegate.available();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
