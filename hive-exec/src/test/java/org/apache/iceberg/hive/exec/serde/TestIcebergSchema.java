/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive.exec.serde;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestIcebergSchema {

  @Test
  public void testIcebergSchema() {
    Assert.assertEquals(
            new Schema(
                    optional(0, "binary_field", Types.BinaryType.get()),
                    optional(1, "byte_field", Types.FixedType.ofLength(1)),
                    optional(2, "boolean_field", Types.BooleanType.get()),
                    optional(3, "char_field", Types.StringType.get()),
                    optional(4, "date_field", Types.DateType.get()),
                    optional(5, "decimal_field", Types.DecimalType.of(38, 18)),
                    optional(6, "double_field", Types.DoubleType.get()),
                    optional(7, "float_field", Types.FloatType.get()),
                    optional(8, "integer_field", Types.IntegerType.get()),
                    optional(9, "long_field", Types.LongType.get()),
                    optional(10, "string_field", Types.StringType.get()),
                    optional(11, "varchar_field", Types.StringType.get()),
                    optional(12, "timestamp_field", Types.TimestampType.withoutZone()),
                    optional(13, "list_field",
                            Types.ListType.ofOptional(16, Types.StringType.get())),
                    optional(14, "map_field",
                            Types.MapType.ofOptional(17, 18, Types.StringType.get(), Types.IntegerType.get())),
                    optional(15, "struct_field", Types.StructType.of(
                            Types.NestedField.optional(19, "nested_field", Types.StringType.get()))))
                    .asStruct(),

            IcebergSchema.create(
                    ImmutableList.of(
                            "binary_field",
                            "byte_field",
                            "boolean_field",
                            "char_field",
                            "date_field",
                            "decimal_field",
                            "double_field",
                            "float_field",
                            "integer_field",
                            "long_field",
                            "string_field",
                            "varchar_field",
                            "timestamp_field",
                            "list_field",
                            "map_field",
                            "struct_field"
                    ),
                    ImmutableList.of(
                            TypeInfoFactory.binaryTypeInfo,
                            TypeInfoFactory.byteTypeInfo,
                            TypeInfoFactory.booleanTypeInfo,
                            TypeInfoFactory.charTypeInfo,
                            TypeInfoFactory.dateTypeInfo,
                            TypeInfoFactory.decimalTypeInfo,
                            TypeInfoFactory.doubleTypeInfo,
                            TypeInfoFactory.floatTypeInfo,
                            TypeInfoFactory.intTypeInfo,
                            TypeInfoFactory.longTypeInfo,
                            TypeInfoFactory.stringTypeInfo,
                            TypeInfoFactory.varcharTypeInfo,
                            TypeInfoFactory.timestampTypeInfo,
                            TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo),
                            TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.intTypeInfo),
                            TypeInfoFactory.getStructTypeInfo(
                                    ImmutableList.of("nested_field"),
                                    ImmutableList.of(TypeInfoFactory.stringTypeInfo))))
                    .asStruct());
  }

}
