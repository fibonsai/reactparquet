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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.nio.spi.s3.S3FileSystemProvider;
import software.amazon.nio.spi.s3.S3XFileSystemProvider;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Optional;

public class SpiAlternativeUtil {

    private static final Logger log = LoggerFactory.getLogger(SpiAlternativeUtil.class);

    public static FileSystem getFileSystem(String uriStr) {
        if (uriStr == null) {
            throw new IllegalArgumentException("uri is null");
        }
        URI uri = URI.create(uriStr);
        String scheme = Optional.ofNullable(uri.getScheme()).orElse("file");
        try {
            return switch (scheme) {
                case "s3" -> {
                    var provider = new S3FileSystemProvider();
                    String bucket = uri.getAuthority();
                    yield provider.getFileSystem(new URI(scheme, bucket, null, null, null));
                }
                case "s3x" -> {
                    var provider = new S3XFileSystemProvider();
                    String userInfo = uri.getUserInfo();
                    String host = uri.getHost();
                    int port = uri.getPort();
                    String[] pathSplit = uri.getPath().split("/");
                    String bucket = pathSplit.length > 0 ? pathSplit[0] : null;
                    yield provider.getFileSystem(new URI(scheme, userInfo, host, port, bucket, null, null));
                }
                case "file" -> FileSystems.getDefault();
                default -> Path.of(uri).getFileSystem();
            };
        } catch (URISyntaxException ex) {
            log.error(ex.getMessage());
        }
        log.warn("URI Schema problem. Fallback to 'file:/'");
        return FileSystems.getDefault();
    }
}
