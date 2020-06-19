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

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.hive.exec.serde.objectinspector.IcebergBinaryObjectInspector;
import org.apache.iceberg.hive.exec.serde.objectinspector.IcebergDateObjectInspector;
import org.apache.iceberg.hive.exec.serde.objectinspector.IcebergRecordObjectInspector;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergSerDe {

  @Test
  public void testEmptyIcebergSerDe() throws SerDeException {
    IcebergSerDe serde = new IcebergSerDe();
    serde.initialize(null, new Properties());

    Assert.assertEquals(IcebergRecordObjectInspector.empty(), serde.getObjectInspector());
  }

  private final List<String> columnNames =
          ImmutableList.of("binary_field", "date_field");
  private final List<TypeInfo> columnTypes =
          ImmutableList.of(TypeInfoFactory.binaryTypeInfo, TypeInfoFactory.dateTypeInfo);

  private final String rawColumnNames = String.join(",", columnNames);
  private final String rawColumnTypes = columnTypes.stream()
                                                   .map(TypeInfo::getTypeName)
                                                   .collect(Collectors.joining(","));

  private final Types.StructType schema = Types.StructType.of(
          Types.NestedField.optional(0, "binary_field", Types.BinaryType.get()),
          Types.NestedField.optional(1, "date_field", Types.DateType.get()));

  private final List<ObjectInspector> ois = ImmutableList.of(
          IcebergBinaryObjectInspector.get(), IcebergDateObjectInspector.get());

  @Test
  public void testIcebergSerDe() throws SerDeException {
    Properties properties = new Properties();
    properties.setProperty(serdeConstants.LIST_COLUMNS, rawColumnNames);
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, rawColumnTypes);

    IcebergSerDe serde = new IcebergSerDe();
    serde.initialize(null, properties);

    Assert.assertEquals(new IcebergRecordObjectInspector(schema, ois), serde.getObjectInspector());
  }

  @Test
  public void testIcebergSerDeWithProjection() throws SerDeException {
    Properties properties = new Properties();
    properties.setProperty(serdeConstants.LIST_COLUMNS, rawColumnNames);
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, rawColumnTypes);

    List<String> projectedColumns = columnNames.subList(0, 1);
    String rawProjectedColumns = String.join(",", projectedColumns);

    Configuration conf = new Configuration();
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, rawProjectedColumns);

    IcebergSerDe serde = new IcebergSerDe();
    serde.initialize(conf, properties);

    Types.StructType projectedSchema = Types.StructType.of(schema.fields().subList(0, 1));
    List<ObjectInspector> projectedOis = ois.subList(0, 1);
    Assert.assertEquals(new IcebergRecordObjectInspector(projectedSchema, projectedOis), serde.getObjectInspector());
  }

}
