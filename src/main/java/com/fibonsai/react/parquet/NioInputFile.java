/*
 *  Copyright (c) 2025 fibonsai.com & blakesmith (thanks)
 *  All rights reserved.
 *
 *  This source is subject to the Apache License, Version 2.0.
 *  Please see the LICENSE file for more information.
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.fibonsai.react.parquet;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

// @Ref: https://blakesmith.me/2024/10/05/how-to-use-parquet-java-without-hadoop.html

/**
 * An {@link org.apache.parquet.io.InputFile} implementation that only uses
 * java.nio.* interfaces for I/O operations. The LocalInputFile implementation in
 * upstream parquet-mr currently falls back to the old-school java file I/O APIs
 * (via Path#toFile) which won't work with nio remote FileSystems such as an S3
 * FileSystem implementation.
 */
public class NioInputFile implements InputFile {
    private final Path path;
    private long length = -1;

    public NioInputFile(Path file) {
        path = file;
    }

    @Override
    public long getLength() throws IOException {
        if (length == -1) {
            length = Files.size(path);
        }
        return length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {

        return new SeekableInputStream() {

            private final SeekableByteChannel byteChannel = Files.newByteChannel(path);
            private final ByteBuffer singleByteBuffer = ByteBuffer.allocate(1);

            @Override
            public int read() throws IOException {
                // There has to be a better way to do this?
                singleByteBuffer.clear();
                final int numRead = read(singleByteBuffer);
                if (numRead >= 0) {
                    return (int)singleByteBuffer.get(0) & 0xFF;
                } else {
                    return -1;
                }
            }

            @Override
            public long getPos() throws IOException {
                return byteChannel.position();
            }

            @Override
            public void seek(long newPos) throws IOException {
                byteChannel.position(newPos);
            }

            @Override
            public void readFully(byte[] bytes) throws IOException {
                readFully(bytes, 0, bytes.length);
            }

            @Override
            public void readFully(byte[] bytes, int start, int len) throws IOException {
                final ByteBuffer buf = ByteBuffer.wrap(bytes);
                buf.position(start);
                readFully(buf);
            }

            @Override
            public int read(ByteBuffer buf) throws IOException {
                return byteChannel.read(buf);
            }

            @Override
            public void readFully(ByteBuffer buf) throws IOException {
                int numRead = 0;
                while (numRead < buf.limit()) {
                    final int code = read(buf);
                    if (code == -1) {
                        return;
                    } else {
                        numRead += code;
                    }
                }
            }

            @Override
            public void close() throws IOException {
                byteChannel.close();
            }
        };
    }
}
