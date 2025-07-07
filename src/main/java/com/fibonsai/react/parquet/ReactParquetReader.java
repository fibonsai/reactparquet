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
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.*;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReactParquetReader {

    private final Configuration conf = new Configuration();

    public Flux<Map<String, Object>> readParquetFile(String filePath) {
        return Flux.generate(
                () -> {
                    final Path path = new Path(filePath);
                    var inputFile = HadoopInputFile.fromPath(path, conf);
                    return ParquetFileReader.open(inputFile);
                },
                (ParquetFileReader reader, SynchronousSink<Tuple2<Long, RecordReader<Map<String, Object>>>> sink) -> {
                    ParquetMetadata metadata = reader.getFooter();
                    MessageType schema = metadata.getFileMetaData().getSchema();
                    PageReadStore pages = null;
                    try {
                        pages = reader.readNextRowGroup();
                    } catch (IOException e) {
                        sink.error(e);
                        throw new RuntimeException(e.getMessage(), e);
                    }
                    if (pages == null) {
                        sink.complete();
                    } else {
                        long rowCount = pages.getRowCount();
                        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                        MapRecordMaterializer materializer = new MapRecordMaterializer(schema);
                        var recordReader = columnIO.getRecordReader(pages, materializer);
                        if (recordReader != null) {
                            sink.next(Tuples.of(rowCount, recordReader));
                        }
                    }
                    return reader;
                },
                (reader) -> {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .flatMap(page -> {
                    long rowCount = page.getT1();
                    var recordReader = page.getT2();
                    return Flux.generate(
                        () -> 0,
                        (count, sink) -> {
                            if (count < rowCount) {
                                var objectMap = recordReader.read();
                                if (objectMap != null) {
                                    sink.next(objectMap);
                                }
                                count++;
                            } else {
                                sink.complete();
                            }
                            return count;
                        });
                });
    }

    private static class MapRecordMaterializer extends RecordMaterializer<Map<String, Object>> {
        private final MapGroupConverter rootConverter;

        public MapRecordMaterializer(MessageType schema) {
            this.rootConverter = new MapGroupConverter(schema);
        }

        @Override
        public Map<String, Object> getCurrentRecord() {
            return rootConverter.getCurrentRecord();
        }

        @Override
        public GroupConverter getRootConverter() {
            return rootConverter;
        }
    }

    private static class MapGroupConverter extends GroupConverter {
        private final Map<String, Object> currentRecord;
        private final List<Converter> converters;

        public MapGroupConverter(MessageType schema) {
            this.currentRecord = new HashMap<>();
            this.converters = new ArrayList<>();

            for (Type field : schema.getFields()) {
                converters.add(createConverter(field, currentRecord));
            }
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converters.get(fieldIndex);
        }

        @Override
        public void start() {
            currentRecord.clear();
        }

        @Override
        public void end() {
            // Record is complete
        }

        public Map<String, Object> getCurrentRecord() {
            return new HashMap<>(currentRecord);
        }

        private Converter createConverter(Type field, Map<String, Object> record) {
            if (field.isPrimitive()) {
                return new MapPrimitiveConverter(field.getName(), field.asPrimitiveType(), record);
            } else {
                return new MapNestedGroupConverter(field.getName(), field.asGroupType(), record);
            }
        }
    }

    private static class MapPrimitiveConverter extends PrimitiveConverter {
        private final String fieldName;
        private final PrimitiveType primitiveType;
        private final Map<String, Object> record;

        public MapPrimitiveConverter(String fieldName, PrimitiveType primitiveType, Map<String, Object> record) {
            this.fieldName = fieldName;
            this.primitiveType = primitiveType;
            this.record = record;
        }

        @Override
        public void addBoolean(boolean value) {
            record.put(fieldName, value);
        }

        @Override
        public void addInt(int value) {
            if (primitiveType.getLogicalTypeAnnotation() != null) {
                switch (primitiveType.getLogicalTypeAnnotation().toString()) {
                    case "DATE":
                        // Convert days since epoch to LocalDate
                        record.put(fieldName, LocalDate.ofEpochDay(value));
                        return;
                    case "TIME(MILLIS,true)":
                    case "TIME(MICROS,true)":
                        record.put(fieldName, value);
                        return;
                }
            }
            record.put(fieldName, value);
        }

        @Override
        public void addLong(long value) {
            if (primitiveType.getLogicalTypeAnnotation() != null) {
                String logicalType = primitiveType.getLogicalTypeAnnotation().toString();
                if (logicalType.startsWith("TIMESTAMP")) {
                    // Convert timestamp to LocalDateTime
                    if (logicalType.contains("MILLIS")) {
                        record.put(fieldName, LocalDateTime.ofEpochSecond(value / 1000,
                                (int) ((value % 1000) * 1_000_000), ZoneOffset.UTC));
                    } else if (logicalType.contains("MICROS")) {
                        record.put(fieldName, LocalDateTime.ofEpochSecond(value / 1_000_000,
                                (int) ((value % 1_000_000) * 1000), ZoneOffset.UTC));
                    } else {
                        record.put(fieldName, LocalDateTime.ofEpochSecond(value / 1_000_000_000,
                                (int) (value % 1_000_000_000), ZoneOffset.UTC));
                    }
                    return;
                }
            }
            record.put(fieldName, value);
        }

        @Override
        public void addFloat(float value) {
            record.put(fieldName, value);
        }

        @Override
        public void addDouble(double value) {
            record.put(fieldName, value);
        }

        @Override
        public void addBinary(Binary value) {
            if (primitiveType.getLogicalTypeAnnotation() != null) {
                String logicalType = primitiveType.getLogicalTypeAnnotation().toString();
                if (logicalType.startsWith("STRING") || logicalType.equals("UTF8")) {
                    record.put(fieldName, value.toStringUsingUTF8());
                    return;
                } else if (logicalType.startsWith("DECIMAL")) {
                    byte[] bytes = value.getBytes();
                    BigInteger bigInteger = new BigInteger(bytes);
                    record.put(fieldName, new BigDecimal(bigInteger));
                    return;
                }
            }

            try {
                record.put(fieldName, value.toStringUsingUTF8());
            } catch (Exception e) {
                record.put(fieldName, value.getBytes());
            }
        }

    }

    private static class MapNestedGroupConverter extends GroupConverter {
        private final String fieldName;
        private final Map<String, Object> parentRecord;
        private final Map<String, Object> nestedRecord;
        private final List<Converter> converters;

        public MapNestedGroupConverter(String fieldName, GroupType groupType, Map<String, Object> parentRecord) {
            this.fieldName = fieldName;
            this.parentRecord = parentRecord;
            this.nestedRecord = new HashMap<>();
            this.converters = new ArrayList<>();

            for (Type field : groupType.getFields()) {
                converters.add(createConverter(field, nestedRecord));
            }
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converters.get(fieldIndex);
        }

        @Override
        public void start() {
            nestedRecord.clear();
        }

        @Override
        public void end() {
            parentRecord.put(fieldName, new HashMap<>(nestedRecord));
        }

        private Converter createConverter(Type field, Map<String, Object> record) {
            if (field.isPrimitive()) {
                return new MapPrimitiveConverter(field.getName(), field.asPrimitiveType(), record);
            } else {
                return new MapNestedGroupConverter(field.getName(), field.asGroupType(), record);
            }
        }
    }
}