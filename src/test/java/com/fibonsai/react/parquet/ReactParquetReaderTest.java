
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

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.file.Paths;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReactParquetReaderTest {

    @Test
    void readParquetFile() {
        ReactParquetReader reader = new ReactParquetReader();
        String parquetFilePath = null;
        try {
            var resource = ParquetUtilsTest.class.getClassLoader().getResource("userdata.parquet");
            if (resource != null) {
                parquetFilePath = Paths.get(resource.toURI()).toAbsolutePath().toString();
            }
            assertNotNull(parquetFilePath);
            Flux<Map<String, Object>> flux = reader.readParquetFile(parquetFilePath);

            StepVerifier.create(flux)
                    .expectNextCount(1000)
                    .verifyComplete();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void readParquetFileBadFile() {
        ReactParquetReader reader = new ReactParquetReader();
        Flux<Map<String, Object>> flux = reader.readParquetFile("badfile.parquet");

        StepVerifier.create(flux)
                .expectError()
                .verify();
    }
}
