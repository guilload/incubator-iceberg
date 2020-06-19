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

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergSerDeParameters {

  @Test
  public void testEmptyIcebergSerDeParameters() {
    Properties properties = new Properties();
    SerDeParameters empty = new SerDeParameters(null, properties);

    Assert.assertEquals(Collections.emptyList(), empty.columnNames());
    Assert.assertEquals(Collections.emptyList(), empty.columnTypes());
    Assert.assertEquals(Collections.emptyList(), empty.projectedColumns());
  }

  @Test
  public void testIcebergSerDeParameters() {
    List<String> columnNames = ImmutableList.of(
            "binary_field",
            "integer_field",
            "string_field",
            "list_field",
            "map_field",
            "struct_field");
    String rawColumnNames = String.join(",", columnNames);

    List<TypeInfo> columnTypes = ImmutableList.of(
            TypeInfoFactory.binaryTypeInfo,
            TypeInfoFactory.intTypeInfo,
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo),
            TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.intTypeInfo),
            TypeInfoFactory.getStructTypeInfo(
                    ImmutableList.of("nested_field"), ImmutableList.of(TypeInfoFactory.stringTypeInfo)));

    String rawColumnTypes = columnTypes.stream().map(TypeInfo::getTypeName).collect(Collectors.joining(","));

    List<String> projectedColumns = columnNames.subList(0, 2);
    String rawProjectedColumns = String.join(",", projectedColumns);

    Properties properties = new Properties();
    properties.setProperty(serdeConstants.LIST_COLUMNS, rawColumnNames);
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, rawColumnTypes);

    Configuration conf = new Configuration();
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, rawProjectedColumns);

    SerDeParameters serDeParameters = new SerDeParameters(conf, properties);

    Assert.assertEquals(columnNames, serDeParameters.columnNames());
    Assert.assertEquals(columnTypes, serDeParameters.columnTypes());
    Assert.assertEquals(projectedColumns, serDeParameters.projectedColumns());
  }

}
