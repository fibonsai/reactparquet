/*
 *  Copyright (c) 2025 fibonsai.com
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

import software.amazon.nio.spi.s3.S3FileSystemProvider;
import software.amazon.nio.spi.s3.S3XFileSystemProvider;

import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SpiAlternativeUtil {

    public static FileSystem getFileSystem(String file) {
        if (file == null) {
            throw new IllegalArgumentException("argument is null");
        }
        if (file.startsWith("s3:")) {
            var provider = new S3FileSystemProvider();
            String rootPath = Stream.of(file.split("/")).limit(3).collect(Collectors.joining("/"));
            System.out.println(rootPath);
            return provider.getFileSystem(URI.create(rootPath));
        }
        if (file.startsWith("s3x:")) {
            var provider = new S3XFileSystemProvider();
            String rootPath = Stream.of(file.split("/")).limit(4).collect(Collectors.joining("/"));
            System.out.println(rootPath);
            return provider.getFileSystem(URI.create(rootPath));
        }
        return FileSystems.getFileSystem(URI.create("file:/"));
    }
}
