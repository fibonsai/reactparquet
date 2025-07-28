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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetUtils {

    public record TypeWithRepetition(String type,
                                     String repetition,
                                     Map<String, TypeWithRepetition> group) {}

    public record FileInfo(String createdBy,
                           String schemaName,
                           long groupsCount,
                           long rowsTotal,
                           Map<String, TypeWithRepetition> fieldsInfo) {}

    public static FileInfo showMetadata(String filePath) {
        Configuration conf = new Configuration();
        Path path = new Path(filePath);

        FileInfo fileInfo = null;
        try {
            final HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);

            try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                ParquetMetadata metadata = reader.getFooter();
                MessageType schema = metadata.getFileMetaData().getSchema();
                long totalRows = metadata.getBlocks().stream()
                        .mapToLong(BlockMetaData::getRowCount)
                        .sum();

                Map<String, TypeWithRepetition> fieldsMap = new HashMap<>();
                showFieldsWithTypes(schema.getFields(), fieldsMap);

                fileInfo = new FileInfo(
                        metadata.getFileMetaData().getCreatedBy(),
                        schema.getName(),
                        metadata.getBlocks().size(),
                        totalRows,
                        fieldsMap
                );
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading parquet file: %s".formatted(filePath), e);
        }
        return fileInfo;
    }

    private static void showFieldsWithTypes(List<Type> fields, Map<String, TypeWithRepetition> fieldsMap) {

        for (Type field : fields) {
            String fieldName = field.getName();
            String javaType = getJavaTypeForField(field);
            String repetition = field.getRepetition().name();
            Map<String, TypeWithRepetition> group = new HashMap<>();

            fieldsMap.put(fieldName, new TypeWithRepetition(javaType, repetition, group));

            if (!field.isPrimitive()) {
                GroupType groupType = field.asGroupType();
                showFieldsWithTypes(groupType.getFields(), group);
            }
        }
    }

    private static String getJavaTypeForField(Type field) {
        if (!field.isPrimitive()) {
            return "Map<String, Object>"; // Nested structure
        }

        PrimitiveType primitiveType = field.asPrimitiveType();

        if (primitiveType.getLogicalTypeAnnotation() != null) {
            String logicalType = primitiveType.getLogicalTypeAnnotation().toString();

            if (logicalType.equals("UTF8") || logicalType.startsWith("STRING")) {
                return "String";
            } else if (logicalType.equals("DATE")) {
                return "LocalDate";
            } else if (logicalType.startsWith("TIMESTAMP")) {
                return "LocalDateTime";
            } else if (logicalType.startsWith("TIME")) {
                return "LocalTime";
            } else if (logicalType.startsWith("DECIMAL")) {
                return "BigDecimal";
            } else if (logicalType.equals("UUID")) {
                return "UUID";
            } else if (logicalType.startsWith("INT")) {
                // INT(8,true) -> Byte, INT(16,true) -> Short, etc.
                if (logicalType.contains("8")) {
                    return "Byte";
                } else if (logicalType.contains("16")) {
                    return "Short";
                } else if (logicalType.contains("32")) {
                    return "Integer";
                } else {
                    return "Long";
                }
            }
        }

        return switch (primitiveType.getPrimitiveTypeName()) {
            case BOOLEAN -> "Boolean";
            case INT32 -> "Integer";
            case INT64 -> "Long";
            case FLOAT -> "Float";
            case DOUBLE -> "Double";
            case BINARY -> "String"; // Default to String, could be byte[]
            case FIXED_LEN_BYTE_ARRAY -> "byte[]";
            case INT96 -> "LocalDateTime"; // Often used for timestamps
            default -> "Object";
        };
    }
}