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

package org.apache.iceberg.hive.exec.serde.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;


public class TestIcebergObjectInspector {

  private final ObjectInspector expectedStringObjectInspector =
          PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(TypeInfoFactory.stringTypeInfo);

  private final Schema schema = new Schema(
          required(1, "binary_field", Types.BinaryType.get(), "binary comment"),
          required(2, "boolean_field", Types.BooleanType.get(), "boolean comment"),
          required(3, "date_field", Types.DateType.get(), "date comment"),
          required(4, "decimal_field", Types.DecimalType.of(38, 18), "decimal comment"),
          required(5, "double_field", Types.DoubleType.get(), "double comment"),
          required(6, "float_field", Types.FloatType.get(), "float comment"),
          required(7, "integer_field", Types.IntegerType.get(), "integer comment"),
          required(8, "long_field", Types.LongType.get(), "long comment"),
          required(9, "string_field", Types.StringType.get(), "string comment"),
          required(10, "timestamp_field", Types.TimestampType.withoutZone(), "timestamp comment"),
          required(11, "timestamptz_field", Types.TimestampType.withZone(), "timestamptz comment"),

          required(12, "list_field",
                  Types.ListType.ofRequired(13, Types.StringType.get()),
                  "list comment"),
          required(14, "map_field",
                  Types.MapType.ofRequired(15, 16, Types.StringType.get(), Types.IntegerType.get()),
                  "map comment"),
          required(17, "struct_field", Types.StructType.of(
                  Types.NestedField.required(18, "nested_field", Types.StringType.get(), "nested field comment")), "struct comment"
          )
  );

  @Test
  public void testIcebergObjectInspector() {
    ObjectInspector oi = IcebergObjectInspector.create(schema);
    Assert.assertNotNull(oi);
    Assert.assertEquals(ObjectInspector.Category.STRUCT, oi.getCategory());

    StructObjectInspector soi = (StructObjectInspector) oi;

    // binary
    StructField binaryField = soi.getStructFieldRef("binary_field");
    Assert.assertEquals(1, binaryField.getFieldID());
    Assert.assertEquals("binary_field", binaryField.getFieldName());
    Assert.assertEquals("binary comment", binaryField.getFieldComment());

    Assert.assertNotNull(binaryField.getFieldObjectInspector());
    Assert.assertEquals(IcebergBinaryObjectInspector.get(), binaryField.getFieldObjectInspector());

    // boolean
    StructField booleanField = soi.getStructFieldRef("boolean_field");
    Assert.assertEquals(2, booleanField.getFieldID());
    Assert.assertEquals("boolean_field", booleanField.getFieldName());
    Assert.assertEquals("boolean comment", booleanField.getFieldComment());

    Assert.assertNotNull(booleanField.getFieldObjectInspector());
    Assert.assertEquals(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(TypeInfoFactory.booleanTypeInfo),
            booleanField.getFieldObjectInspector());

    // date
    StructField dateField = soi.getStructFieldRef("date_field");
    Assert.assertEquals(3, dateField.getFieldID());
    Assert.assertEquals("date_field", dateField.getFieldName());
    Assert.assertEquals("date comment", dateField.getFieldComment());

    Assert.assertNotNull(dateField.getFieldObjectInspector());
    Assert.assertEquals(IcebergDateObjectInspector.get(), dateField.getFieldObjectInspector());

    // decimal
    StructField decimalField = soi.getStructFieldRef("decimal_field");
    Assert.assertEquals(4, decimalField.getFieldID());
    Assert.assertEquals("decimal_field", decimalField.getFieldName());
    Assert.assertEquals("decimal comment", decimalField.getFieldComment());

    Assert.assertNotNull(decimalField.getFieldObjectInspector());
    Assert.assertEquals(IcebergDecimalObjectInspector.get(38, 18), decimalField.getFieldObjectInspector());

    // double
    StructField doubleField = soi.getStructFieldRef("double_field");
    Assert.assertEquals(5, doubleField.getFieldID());
    Assert.assertEquals("double_field", doubleField.getFieldName());
    Assert.assertEquals("double comment", doubleField.getFieldComment());

    Assert.assertNotNull(doubleField.getFieldObjectInspector());
    Assert.assertEquals(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(TypeInfoFactory.doubleTypeInfo),
            doubleField.getFieldObjectInspector());

    // float
    StructField floatField = soi.getStructFieldRef("float_field");
    Assert.assertEquals(6, floatField.getFieldID());
    Assert.assertEquals("float_field", floatField.getFieldName());
    Assert.assertEquals("float comment", floatField.getFieldComment());

    Assert.assertNotNull(floatField.getFieldObjectInspector());
    Assert.assertEquals(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(TypeInfoFactory.floatTypeInfo),
            floatField.getFieldObjectInspector());

    // integer
    StructField integerField = soi.getStructFieldRef("integer_field");
    Assert.assertEquals(7, integerField.getFieldID());
    Assert.assertEquals("integer_field", integerField.getFieldName());
    Assert.assertEquals("integer comment", integerField.getFieldComment());

    Assert.assertNotNull(integerField.getFieldObjectInspector());
    Assert.assertEquals(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(TypeInfoFactory.intTypeInfo),
            integerField.getFieldObjectInspector());

    // long
    StructField longField = soi.getStructFieldRef("long_field");
    Assert.assertEquals(8, longField.getFieldID());
    Assert.assertEquals("long_field", longField.getFieldName());
    Assert.assertEquals("long comment", longField.getFieldComment());

    Assert.assertNotNull(longField.getFieldObjectInspector());
    Assert.assertEquals(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(TypeInfoFactory.longTypeInfo),
            longField.getFieldObjectInspector());

    // string
    StructField stringField = soi.getStructFieldRef("string_field");
    Assert.assertEquals(9, stringField.getFieldID());
    Assert.assertEquals("string_field", stringField.getFieldName());
    Assert.assertEquals("string comment", stringField.getFieldComment());

    Assert.assertNotNull(stringField.getFieldObjectInspector());
    Assert.assertEquals(expectedStringObjectInspector, stringField.getFieldObjectInspector());

    // timestamp without tz
    StructField timestampField = soi.getStructFieldRef("timestamp_field");
    Assert.assertEquals(10, timestampField.getFieldID());
    Assert.assertEquals("timestamp_field", timestampField.getFieldName());
    Assert.assertEquals("timestamp comment", timestampField.getFieldComment());

    Assert.assertNotNull(timestampField.getFieldObjectInspector());
    Assert.assertEquals(IcebergTimestampObjectInspector.get(false), timestampField.getFieldObjectInspector());

    // timestamp with tz
    StructField timestampTzField = soi.getStructFieldRef("timestamptz_field");
    Assert.assertEquals(11, timestampTzField.getFieldID());
    Assert.assertEquals("timestamptz_field", timestampTzField.getFieldName());
    Assert.assertEquals("timestamptz comment", timestampTzField.getFieldComment());

    Assert.assertNotNull(timestampTzField.getFieldObjectInspector());
    Assert.assertEquals(IcebergTimestampObjectInspector.get(true), timestampTzField.getFieldObjectInspector());

    // list
    StructField listField = soi.getStructFieldRef("list_field");
    Assert.assertEquals(12, listField.getFieldID());
    Assert.assertEquals("list_field", listField.getFieldName());
    Assert.assertEquals("list comment", listField.getFieldComment());

    Assert.assertNotNull(listField.getFieldObjectInspector());
    Assert.assertEquals(
            ObjectInspectorFactory.getStandardListObjectInspector(expectedStringObjectInspector),
            listField.getFieldObjectInspector());

    // map
    StructField mapField = soi.getStructFieldRef("map_field");
    Assert.assertEquals(14, mapField.getFieldID());
    Assert.assertEquals("map_field", mapField.getFieldName());
    Assert.assertEquals("map comment", mapField.getFieldComment());

    Assert.assertNotNull(mapField.getFieldObjectInspector());
    Assert.assertEquals(
            ObjectInspectorFactory.getStandardMapObjectInspector(expectedStringObjectInspector,
                    PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(TypeInfoFactory.intTypeInfo)),
            mapField.getFieldObjectInspector());

    // struct
    StructField structField = soi.getStructFieldRef("struct_field");
    Assert.assertEquals(17, structField.getFieldID());
    Assert.assertEquals("struct_field", structField.getFieldName());
    Assert.assertEquals("struct comment", structField.getFieldComment());

    Assert.assertNotNull(structField.getFieldObjectInspector());
    Assert.assertEquals(
            new IcebergRecordObjectInspector(
                    (Types.StructType) schema.findType(17), ImmutableList.of(expectedStringObjectInspector)),
            structField.getFieldObjectInspector());
  }

  @Test
  public void testIcebergObjectInspectorUnsupportedTypes() {
    // unsupported types
    AssertHelpers.assertThrows(
            "Hive does not support fixed type",
            IllegalArgumentException.class,
            "FIXED type is not supported",
            () -> IcebergObjectInspector.create(new Schema(required(1, "fixed_field", Types.FixedType.ofLength(1))))
    );

    AssertHelpers.assertThrows(
            "Hive does not support time type",
            IllegalArgumentException.class,
            "TIME type is not supported",
            () -> IcebergObjectInspector.create(new Schema(required(1, "time_field", Types.TimeType.get())))
    );

    AssertHelpers.assertThrows(
            "Hive does not support UUID type",
            IllegalArgumentException.class,
            "UUID type is not supported",
            () -> IcebergObjectInspector.create(new Schema(required(1, "uuid_field", Types.UUIDType.get())))
    );
  }

}
