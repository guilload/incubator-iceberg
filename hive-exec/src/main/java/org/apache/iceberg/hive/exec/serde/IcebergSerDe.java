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
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hive.exec.serde.objectinspector.IcebergObjectInspector;
import org.apache.iceberg.mr.IcebergMRConfig;
import org.apache.iceberg.mr.mapred.Container;

public final class IcebergSerDe extends AbstractSerDe {

  private ObjectInspector inspector;

  @Override
  public void initialize(@Nullable Configuration conf, Properties properties) throws SerDeException {
    SerDeParameters serDeParameters = new SerDeParameters(conf, properties);
    Schema schema = IcebergSchema.create(serDeParameters.columnNames(), serDeParameters.columnTypes());

    List<String> projectedColumns = serDeParameters.projectedColumns();
    Schema projection = projectedColumns.isEmpty() ? schema : schema.select(projectedColumns);

    inspector = IcebergObjectInspector.create(projection);

    if (conf != null) {
      IcebergMRConfig.setSchema(conf, schema);
      IcebergMRConfig.setProjection(conf, schema);
    }
  }

  @Override
  public Object deserialize(Writable writable) {
    return ((Container<?>) writable).get();
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Container.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    throw new UnsupportedOperationException("serialize is not supported");
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }
}
