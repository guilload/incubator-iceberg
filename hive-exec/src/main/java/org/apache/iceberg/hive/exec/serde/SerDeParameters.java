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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public final class SerDeParameters {

  private static final String DEFAULT_DELIMITER = String.valueOf(SerDeUtils.COMMA);

  private final List<String> columnNames;
  private final List<TypeInfo> columnTypes;
  private final List<String> projectedColumns;

  public SerDeParameters(@Nullable Configuration conf, Properties properties) {
    this.columnNames = parseColumnNames(properties);
    this.columnTypes = parseColumnTypes(properties);
    this.projectedColumns = parseProjectedColumns(conf);
  }

  public List<String> columnNames() {
    return columnNames;
  }

  public List<TypeInfo> columnTypes() {
    return columnTypes;
  }

  public List<String> projectedColumns() {
    return projectedColumns;
  }

  private static List<String> parseColumnNames(Properties properties) {
    String rawColumnNames  = properties.getProperty(serdeConstants.LIST_COLUMNS);
    String delimiter = properties.getProperty(serdeConstants.COLUMN_NAME_DELIMITER, DEFAULT_DELIMITER);

    if (rawColumnNames == null || rawColumnNames.isEmpty()) {
      return Collections.emptyList();
    }

    return Collections.unmodifiableList(Arrays.asList(rawColumnNames.split(delimiter)));
  }

  private static List<TypeInfo> parseColumnTypes(Properties properties) {
    String rawColumnTypes = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    if (rawColumnTypes == null || rawColumnTypes.isEmpty()) {
      return Collections.emptyList();
    }

    return Collections.unmodifiableList(TypeInfoUtils.getTypeInfosFromTypeString(rawColumnTypes));
  }

  private static List<String> parseProjectedColumns(Configuration conf) {
    if (conf == null) {
      return Collections.emptyList();
    }

    return Collections.unmodifiableList(Arrays.asList(ColumnProjectionUtils.getReadColumnNames(conf)));
  }

}
