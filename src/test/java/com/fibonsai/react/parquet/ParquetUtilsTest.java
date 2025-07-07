
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

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class ParquetUtilsTest {

    @Test
    void showMetadata() {
        String parquetFilePath = null;
        try {
            var resource = ParquetUtilsTest.class.getClassLoader().getResource("userdata.parquet");
            if (resource != null) {
                parquetFilePath = Paths.get(resource.toURI()).toAbsolutePath().toString();
            }
            assertNotNull(parquetFilePath);
            ParquetUtils.FileInfo fileInfo = ParquetUtils.showMetadata(parquetFilePath);
            assertNotNull(fileInfo);
            assertEquals("parquet-mr version 1.8.1 (build 4aba4dae7bb0d4edbcf7923ae1339f28fd3f7fcf)", fileInfo.createdBy());
            assertEquals("hive_schema", fileInfo.schemaName());
            assertEquals(1, fileInfo.groupsCount());
            assertEquals(1000, fileInfo.rowsTotal());
            assertNotNull(fileInfo.fieldsInfo());
            assertEquals(13, fileInfo.fieldsInfo().size());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void showMetadataBadFile() {
        assertThrows(RuntimeException.class, () -> ParquetUtils.showMetadata("badfile.parquet"));
    }
}
