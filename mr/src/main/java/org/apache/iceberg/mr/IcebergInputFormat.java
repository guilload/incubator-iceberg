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

package org.apache.iceberg.mr;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class IcebergInputFormat<T> extends InputFormat<Void, T> {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergInputFormat.class);

  private static final String ICEBERG_FILTER_EXPRESSION = "iceberg.filter.expression";
  private static final String ICEBERG_PROJECTED_FIELDS = "iceberg.projected.fields";
  private static final String ICEBERG_TABLE_IDENTIFIER = "iceberg.table.identifier";
  private static final String ICEBERG_TABLE_SCHEMA = "iceberg.table.schema";

  private Catalog catalog;
  private Table table;
  private List<InputSplit> splits;

  IcebergInputFormat() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    if (splits != null) {
      LOG.info("Returning cached splits: " + splits.size());
      return splits;
    }

    TableScan scan = getTable(context).newScan();

    // Apply filters
    Expression expression = (Expression) deserialize(context.getConfiguration().get(ICEBERG_FILTER_EXPRESSION));

    if (expression != null) {
      LOG.info("Filter expression: " + expression);
      scan = scan.filter(expression);
    }

    // Wrap in splits
    splits = Lists.newArrayList();

    try (CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
      tasks.forEach((scanTask) -> splits.add(new IcebergSplit(scanTask)));
    }

    return splits;
  }

  @Override
  public RecordReader<Void, T> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new IcebergRecordReader();
  }

  private static class IcebergSplit extends InputSplit implements Writable {

    private static final String[] ANYWHERE = new String[] { "*" };

    private CombinedScanTask task;

    IcebergSplit(CombinedScanTask task) {
      this.task = task;
    }

    public IcebergSplit() {

    }

    @Override
    public long getLength() {
      return task.files().stream().mapToLong(FileScanTask::length).sum();
    }

    @Override
    public String[] getLocations() {
      return ANYWHERE;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      byte[] data = SerializationUtils.serialize(this.task);
      out.writeInt(data.length);
      out.write(data);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      byte[] data = new byte[in.readInt()];
      in.readFully(data);

      this.task = (CombinedScanTask) SerializationUtils.deserialize(data);
    }
  }

  public class IcebergRecordReader extends RecordReader<Void, T> {

    private TaskAttemptContext context;

    private Iterator<FileScanTask> tasks;
    private CloseableIterable<T> reader;
    private Iterator<T> recordIterator;
    private T currentRecord;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
      this.context = context;

      CombinedScanTask task = ((IcebergSplit) split).task;
      tasks = task.files().iterator();

      advance();
    }

    @SuppressWarnings("unchecked")
    private boolean advance() throws IOException {
      if (reader != null) {
        reader.close();
      }

      if (!tasks.hasNext()) {
        return false;
      }

      FileScanTask currentTask = tasks.next();
      DataFile file = currentTask.file();
      InputFile inputFile = HadoopInputFile.fromLocation(file.path(), context.getConfiguration());
      Schema tableSchema = SchemaParser.fromJson(context.getConfiguration().get(ICEBERG_TABLE_SCHEMA));
      boolean reuseContainers = true; // FIXME: read from config

      // TODO: handle projected fields and join partition data from the manifest file

      switch (file.format()) {
        case AVRO:
          reader = buildAvroReader(currentTask, inputFile, tableSchema, reuseContainers);
          break;
        case ORC:
          reader = buildOrcReader(currentTask, inputFile, tableSchema, reuseContainers);
          break;
        case PARQUET:
          reader = buildParquetReader(currentTask, inputFile, tableSchema, reuseContainers);
          break;

        default:
          throw new UnsupportedOperationException(String.format("Cannot read %s file: %s", file.format().name(), file.path()));
      }

      recordIterator = reader.iterator();
      return true;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      if (recordIterator.hasNext()) {
        currentRecord = recordIterator.next();
        return true;
      }

      while (advance()) {
        if (recordIterator.hasNext()) {
          currentRecord = recordIterator.next();
          return true;
        }
      }

      return false;
    }

    @Override
    public Void getCurrentKey() {
      return null;
    }

    @Override
    public T getCurrentValue() {
      return currentRecord;
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void close() throws IOException {
      if (reader != null) {
        reader.close();
      }
    }

  }

  // FIXME: use generic reader function
  private static CloseableIterable buildAvroReader(FileScanTask task, InputFile file, Schema schema, boolean reuseContainers) {
    Avro.ReadBuilder builder = Avro.read(file)
            .createReaderFunc(DataReader::create)
            .project(schema)
            .split(task.start(), task.length());

    if (reuseContainers) {
      builder.reuseContainers();
    }

    return builder.build();
  }

  // FIXME: use generic reader function
  private static CloseableIterable buildOrcReader(FileScanTask task, InputFile file, Schema schema, boolean reuseContainers) {
    ORC.ReadBuilder builder = ORC.read(file)
//            .createReaderFunc() // FIXME: implement
            .schema(schema)
            .split(task.start(), task.length());

    return builder.build();
  }

  // FIXME: use generic reader function
  private static CloseableIterable buildParquetReader(FileScanTask task, InputFile file, Schema schema, boolean reuseContainers) {
    Parquet.ReadBuilder builder = Parquet.read(file)
            .createReaderFunc(messageType -> GenericParquetReaders.buildReader(schema, messageType))
            .project(schema)
            .split(task.start(), task.length());

    if (reuseContainers) {
      builder.reuseContainers();
    }

    return builder.build();
  }

  private Catalog getCatalog(JobContext context) {
    if (catalog == null) {
      synchronized (this) {
        if (catalog == null) {
          catalog = HiveCatalogs.loadCatalog(context.getConfiguration());
        }
      }
    }

    return catalog;
  }

  private Table getTable(JobContext context) {
    if (table == null) {
      synchronized (this) {
        if (table == null) {
          TableIdentifier identifier = TableIdentifier.parse(context.getConfiguration().get(ICEBERG_TABLE_IDENTIFIER));
          table = getCatalog(context).loadTable(identifier);
        }
      }
    }

    return table;
  }

  public static void setFilterExpression(Job job, Expression expression) throws IOException {
    job.getConfiguration().set(ICEBERG_FILTER_EXPRESSION, serialize(expression));
  }

  public static void setProjectedFields(Job job, List<String> fields) {
    setProjectedFields(job, fields.toArray(new String[0]));
  }

  public static void setProjectedFields(Job job, String... fields) {
    job.getConfiguration().setStrings(ICEBERG_PROJECTED_FIELDS, fields);
  }

  public static void setTableIdentifier(Job job, String identifier) {
    job.getConfiguration().set(ICEBERG_TABLE_IDENTIFIER, identifier);
  }

  public static void setSchema(Job job, Schema schema) {
    job.getConfiguration().set(ICEBERG_TABLE_SCHEMA, SchemaParser.toJson(schema));
  }


  private static String serialize(Serializable object) throws IOException {
    if (object == null) {
      return "";
    }

    ByteArrayOutputStream byteStream;
    ObjectOutputStream objectStream = null;

    try {
      Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
      byteStream = new ByteArrayOutputStream();
      objectStream = new ObjectOutputStream(new DeflaterOutputStream(byteStream, deflater));
      objectStream.writeObject(object);
      return encodeBytes(byteStream.toByteArray());
    } catch (Exception e) {
      throw new IOException("Serialization error: " + e, e);
    } finally {
      IOUtils.closeQuietly(objectStream);
    }
  }

  private static Object deserialize(String string) throws IOException {
    if (StringUtils.isBlank(string)) {
      return null;
    }

    ObjectInputStream objectStream = null;

    try {
      ByteArrayInputStream byteStream = new ByteArrayInputStream(decodeBytes(string));
      objectStream = new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), new InflaterInputStream(byteStream));
      return objectStream.readObject();
    } catch (Exception e) {
      throw new IOException("Deserialization error: " + e, e);
    } finally {
      IOUtils.closeQuietly(objectStream);
    }
  }

  private static String encodeBytes(byte[] bytes) {
    return bytes == null ? null : new String(Base64.encodeBase64(bytes), StandardCharsets.UTF_8);
  }

  private static byte[] decodeBytes(String string) {
    return Base64.decodeBase64(string.getBytes(StandardCharsets.UTF_8));
  }

}
