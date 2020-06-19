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
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Visitor for Hive TypeInfo.
 * @param <T> return type of the visitor
 */
public class TypeInfoVisitor<T> {

  public T struct(StructTypeInfo struct, List<T> fields) {
    return null;
  }

  public T list(ListTypeInfo list, T element) {
    return null;
  }

  public T map(MapTypeInfo map, T key, T value) {
    return null;
  }

  public T primitive(PrimitiveTypeInfo primitive) {
    return null;
  }

  public static <T> T visit(TypeInfo typeInfo, TypeInfoVisitor<T> visitor) {
    switch (typeInfo.getCategory()) {
      case LIST:
        ListTypeInfo list = (ListTypeInfo) typeInfo;
        T element = visit(list.getListElementTypeInfo(), visitor);
        return visitor.list(list, element);

      case MAP:
        MapTypeInfo map = (MapTypeInfo) typeInfo;
        T key = visit(map.getMapKeyTypeInfo(), visitor);
        T value = visit(map.getMapValueTypeInfo(), visitor);
        return visitor.map(map, key, value);

      case PRIMITIVE:
        return visitor.primitive((PrimitiveTypeInfo) typeInfo);

      case STRUCT:
        StructTypeInfo struct = (StructTypeInfo) typeInfo;
        List<T> fields = Lists.newArrayListWithExpectedSize(struct.getAllStructFieldNames().size());

        for (TypeInfo ti : struct.getAllStructFieldTypeInfos()) {
          T field = visit(ti, visitor);
          fields.add(field);
        }
        return visitor.struct(struct, fields);

      default:
        throw new IllegalArgumentException(typeInfo.getCategory() + " type is not supported");
    }
  }

}
