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
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Visit a Hive StructTypeInfo and return the corresponding Iceberg Schema.
 */
public class IcebergSchema extends TypeInfoVisitor<Type> {

  private final StructTypeInfo root;
  private int currentId;

  private IcebergSchema(StructTypeInfo root) {
    this.root = root;
    // IDs in the range [0, root.getAllStructFieldNames().size()[ are reserved for top-level fields.
    this.currentId = root.getAllStructFieldNames().size();
  }

  public static Schema create(List<String> columnNames, List<TypeInfo> columnTypes) {
    Preconditions.checkArgument(columnNames.size() == columnTypes.size());

    StructTypeInfo root = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    Type type = visit(root, new IcebergSchema(root));
    return new Schema(type.asStructType().fields());
  }

  @Override
  public Types.ListType list(ListTypeInfo list, Type elementType) {
    return Types.ListType.ofOptional(nextId(), elementType);
  }

  @Override
  public Types.MapType map(MapTypeInfo map, Type keyType, Type valueType) {
    return Types.MapType.ofOptional(nextId(), nextId(), keyType, valueType);
  }

  @Override
  public Type.PrimitiveType primitive(PrimitiveTypeInfo primitiveTypeInfo) {
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
      case BINARY:
        return Types.BinaryType.get();
      case BOOLEAN:
        return Types.BooleanType.get();
      case BYTE:
        return Types.FixedType.ofLength(1);
      case CHAR:
      case STRING:
      case VARCHAR:
        return Types.StringType.get();
      case DATE:
        return Types.DateType.get();
      case DECIMAL:
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
        return Types.DecimalType.of(decimalTypeInfo.precision(), decimalTypeInfo.scale());
      case DOUBLE:
        return Types.DoubleType.get();
      case FLOAT:
        return Types.FloatType.get();
      case INT:
      case SHORT:
        return Types.IntegerType.get();
      case LONG:
        return Types.LongType.get();
      case TIMESTAMP:
        return Types.TimestampType.withoutZone();
      case INTERVAL_DAY_TIME:
      case INTERVAL_YEAR_MONTH:
      case UNKNOWN:
      case VOID:
      default:
        throw new IllegalArgumentException(primitiveTypeInfo.getPrimitiveCategory() + " type is not supported");
    }
  }

  @Override
  public Types.StructType struct(StructTypeInfo struct, List<Type> types) {
    Preconditions.checkArgument(struct.getAllStructFieldNames().size() == types.size());

    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(struct.getAllStructFieldNames().size());
    int idx = 0;

    for (String name : struct.getAllStructFieldNames()) {
      int nextId = struct == root ? idx : nextId();
      Types.NestedField field = Types.NestedField.optional(nextId, name, types.get(idx++));
      fields.add(field);
    }

    return Types.StructType.of(fields);
  }

  private int nextId() {
    return currentId++;
  }

}
